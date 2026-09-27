//! Whole-work translation through the external `json-page-translate` binary.
//!
//! A single action runs three phases in order: detection + OCR, whole-work
//! translation over every page at once, then inpainting. The translator owns
//! chunking, retry, salvage, resume, and JSON repair; this module only builds
//! the input document, spawns the process, relays progress, and imports the
//! result. See the integration spec for the binary contract.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context as _, Result, anyhow, bail};
use koharu_desktop::Desktop;
use koharu_pipeline::{
    Committer, Operation, Pipeline, Progress, ProgressSink, Request, RunStatus, Scope, Stage,
    StageOutput, StopToken,
};
use koharu_scene::{EntityId, Generation, LanguageTag, ProducerId, Snapshot};
use koharu_secrets::ExposeSecret as _;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use specta::Type;
use tauri::{AppHandle, Manager as _, State};
use tauri_runtime_cef::CefRuntime;
use tokio::io::{AsyncBufReadExt as _, BufReader};

use super::{
    ChannelExt as _, Error,
    canvas::CanvasChannel,
    output::{TextExport, TextExportKind, TextExportOrigin, TextExportPage},
    preferences::Preferences,
    processing::{Job, JobChannel, JobId, JobState, Processing},
    project::CurrentProject,
};

const PRODUCER: &str = "dev.koharu.full_context.translation";
const EXECUTABLE: &str = "json-page-translate";
const CONFIG_SECTION: &str = "full_context";
const PROTOCOL_VERSION: &str = "1";
const STOP_POLL: Duration = Duration::from_millis(50);
const DEBUG_LOG_FILE: &str = "full-context-debug.jsonl";

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, Type)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ResponseFormat {
    #[default]
    JsonSchema,
    JsonObject,
    Prompt,
}

impl ResponseFormat {    const fn as_str(self) -> &'static str {
        match self {
            Self::JsonSchema => "json_schema",
            Self::JsonObject => "json_object",
            Self::Prompt => "prompt",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize, Type)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProgressMode {
    None,
    #[default]
    Status,
    Preview,
}

impl ProgressMode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Status => "status",
            Self::Preview => "preview",
        }
    }
}

/// Whole-work translation settings. Loaded from the `[full_context]` section of
/// Koharu's TOML config. The API key is intentionally absent: a Koharu-managed
/// credential is forwarded when present, otherwise the binary reads its own
/// `.env`/environment.
#[derive(Clone, Debug, Deserialize, Serialize, Type)]
#[serde(default)]
pub(crate) struct FullContextConfig {
    /// Explicit binary path. `JSON_PAGE_TRANSLATE_BIN`, the app resources
    /// directory, the executable directory, then `PATH` are tried otherwise.
    pub binary_path: Option<String>,
    pub provider: String,
    /// Falls back to the selected OpenRouter translation model when unset.
    pub model: Option<String>,
    pub base_url: String,
    pub allowed_providers: Vec<String>,
    pub response_format: ResponseFormat,
    pub system_prompt: Option<String>,
    pub system_prompt_file: Option<String>,
    pub batch_pages: u32,
    /// `<N>` or `all`.
    pub context_pages: String,
    pub max_attempts: u32,
    pub transport_retries: u32,
    pub max_output_tokens: Option<u32>,
    pub temperature: Option<f32>,
    pub require_parameter_support: bool,
    pub progress: ProgressMode,
}

impl Default for FullContextConfig {
    fn default() -> Self {
        Self {
            binary_path: None,
            provider: "openrouter".to_owned(),
            model: None,
            base_url: "https://openrouter.ai/api/v1".to_owned(),
            allowed_providers: Vec::new(),
            response_format: ResponseFormat::JsonSchema,
            system_prompt: None,
            system_prompt_file: None,
            batch_pages: 300,
            context_pages: "all".to_owned(),
            max_attempts: 4,
            transport_retries: 3,
            max_output_tokens: None,
            temperature: None,
            require_parameter_support: false,
            progress: ProgressMode::Preview,
        }
    }
}

fn load_config() -> Result<FullContextConfig> {
    let config = koharu_config::load::<FullContextConfig>(CONFIG_SECTION)?;
    Ok(config.read()?.clone())
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum FullContextProgress {
    Total { total: usize },
    Page {
        completed: usize,
        total: usize,
        page: Option<EntityId>,
        /// For a streaming page: whether it was detected from the model's
        /// reasoning or from the JSON answer.
        source: Option<String>,
    },
    /// Progress from the in-process pipeline (detection/OCR, inpainting).
    Pipeline {
        completed: Option<usize>,
        total: Option<usize>,
        page: Option<EntityId>,
        stage: Option<Stage>,
        model: Option<String>,
    },
    /// Pages the translator is currently working on.
    Batch { pages: Vec<u32> },
    /// One provider reply, for live diagnostics: model and cost only.
    Response {
        cost: Option<f64>,
        model: Option<String>,
        provider: Option<String>,
    },
    Done { failed: Vec<u32> },
}

pub(crate) type ProgressReporter = Arc<dyn Fn(FullContextProgress) + Send + Sync>;

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct Outcome {
    pub stopped: bool,
    pub translated_pages: usize,
    pub failed_pages: Vec<u32>,
}

struct TranslationDefaults {
    target: String,
    instructions: Option<String>,
}

#[derive(Default)]
struct TranslationRun {
    output: Option<TextExport>,
    failed_pages: Vec<u32>,
    stopped: bool,
}

/// Runs the three-phase whole-work translation over the entire project.
///
/// Whole-work translation always covers every page: cross-page context and
/// per-page numbering only make sense against the complete work.
pub(crate) async fn execute(
    handle: &AppHandle<CefRuntime>,
    stop: StopToken,
    progress: ProgressReporter,
) -> Result<Outcome> {
    let pipeline = handle.state::<Pipeline>().inner().clone();
    let mut committer = FullContextCommitter {
        handle: handle.clone(),
    };

    // Phase 1: detection + OCR.
    let snapshot = current_snapshot(handle).await?;
    let report = pipeline
        .execute(
            snapshot,
            Request {
                operation: Operation::Through { stage: Stage::Ocr },
                scope: Scope::Project,
                stop: stop.clone(),
                progress: Some(pipeline_progress(&progress)),
                inpainting_mask: None,
            },
            &mut committer,
        )
        .await
        .map_err(anyhow::Error::from)?;
    if report.status == RunStatus::Stopped || stop.stopped() {
        return Ok(Outcome {
            stopped: true,
            ..Outcome::default()
        });
    }

    // Phase 2: whole-work translation through the external binary.
    let snapshot = current_snapshot(handle).await?;
    let (document, page_map) = build_document(&snapshot)?;
    let has_text = document
        .pages
        .iter()
        .any(|page| page.texts.iter().any(|text| !text.trim().is_empty()));

    let mut outcome = Outcome::default();
    if has_text {
        let config = load_config()?;
        let defaults = translation_defaults()?;
        let run = translate_document(
            handle,
            &config,
            config.model.as_deref(),
            &defaults,
            &document,
            &page_map,
            &stop,
            Arc::clone(&progress),
        )
        .await?;
        if run.stopped || stop.stopped() {
            return Ok(Outcome {
                stopped: true,
                ..outcome
            });
        }
        outcome.failed_pages = run.failed_pages.clone();
        if let Some(output) = run.output.as_ref() {
            outcome.translated_pages = import_translations(handle, output).await?;
        }
    }

    if stop.stopped() {
        return Ok(Outcome {
            stopped: true,
            ..outcome
        });
    }

    // Phase 3: inpainting. Only pages that actually carry translations.
    if has_text && outcome.translated_pages > 0 {
        let snapshot = current_snapshot(handle).await?;
        let translated = pages_with_translations(&snapshot)?;
        if !translated.is_empty() {
            let report = pipeline
                .execute(
                    snapshot,
                    Request {
                        operation: Operation::Only {
                            stage: Stage::Inpainting,
                        },
                        scope: Scope::Pages(translated),
                        stop: stop.clone(),
                        progress: Some(pipeline_progress(&progress)),
                        inpainting_mask: None,
                    },
                    &mut committer,
                )
                .await
                .map_err(anyhow::Error::from)?;
            if report.status == RunStatus::Stopped || stop.stopped() {
                outcome.stopped = true;
            }
        }
    }

    Ok(outcome)
}

fn pages_with_translations(snapshot: &Snapshot) -> Result<Vec<EntityId>> {
    let mut translated = Vec::new();
    for page in snapshot.pages() {
        let page_id = page.id();
        let mut has_translation = false;
        if let Some(group) = page.text_group()? {
            for layer in group.text_layers()? {
                if layer.content()?.translation()?.is_some() {
                    has_translation = true;
                    break;
                }
            }
        }
        if has_translation {
            translated.push(page_id);
        }
    }
    Ok(translated)
}

async fn current_snapshot(handle: &AppHandle<CefRuntime>) -> Result<Snapshot> {
    let current = handle.state::<CurrentProject>();
    let current = current.project.lock().await;
    Ok(current
        .as_ref()
        .context("no project is open")?
        .snapshot())
}

fn build_document(snapshot: &Snapshot) -> Result<(TextExport, BTreeMap<u32, EntityId>)> {
    // Page numbers are 1-based positions over `snapshot.pages()`, matching the
    // shared importer and exporter.
    let mut document = Vec::new();
    let mut page_map = BTreeMap::new();
    for (index, page_id) in snapshot.pages().map(|page| page.id()).enumerate() {
        let number = u32::try_from(index + 1).context("too many pages")?;
        let page = snapshot.page(page_id)?;
        let mut texts = Vec::new();
        if let Some(group) = page.text_group()? {
            for layer in group.text_layers()? {
                let content = layer.content()?;
                texts.push(
                    content
                        .source()?
                        .map_or_else(String::new, |source| source.text.value),
                );
            }
        }
        document.push(TextExportPage {
            page: index + 1,
            texts,
        });
        page_map.insert(number, page_id);
    }
    Ok((TextExport { pages: document }, page_map))
}

fn translation_defaults() -> Result<TranslationDefaults> {
    let preferences = Preferences::load()?;
    let translation = &preferences.pipeline.translation;
    Ok(TranslationDefaults {
        target: translation.target_language.tag().to_owned(),
        instructions: translation.instructions.clone(),
    })
}

#[allow(clippy::too_many_arguments)]
async fn translate_document(
    handle: &AppHandle<CefRuntime>,
    config: &FullContextConfig,
    model: Option<&str>,
    defaults: &TranslationDefaults,
    document: &TextExport,
    page_map: &BTreeMap<u32, EntityId>,
    stop: &StopToken,
    progress: ProgressReporter,
) -> Result<TranslationRun> {
    let binary = resolve_binary(handle, config.binary_path.as_deref());
    check_protocol(&binary).await;

    let directory = tempfile::tempdir().context("failed to create a translation workspace")?;
    let input_path = directory.path().join("input.json");
    let output_path = directory.path().join("output.json");
    let state_path = directory.path().join("state.json");
    tokio::fs::write(&input_path, serde_json::to_vec_pretty(document)?)
        .await
        .context("failed to write the translation input")?;

    if config.provider != "openrouter" && config.provider != "fake" {
        bail!(
            "unsupported whole-work translation provider {:?}",
            config.provider
        );
    }

    let mut command = tokio::process::Command::new(&binary);
    command
        .arg(&input_path)
        .arg("--output")
        .arg(&output_path)
        .arg("--state")
        .arg(&state_path)
        .arg("--target")
        .arg(&defaults.target)
        .arg("--provider")
        .arg(&config.provider)
        .arg("--base-url")
        .arg(&config.base_url)
        .arg("--response-format")
        .arg(config.response_format.as_str())
        .arg("--batch-pages")
        .arg(config.batch_pages.max(1).to_string())
        .arg("--context-pages")
        .arg(&config.context_pages)
        .arg("--max-attempts")
        .arg(config.max_attempts.max(1).to_string())
        .arg("--transport-retries")
        .arg(config.transport_retries.to_string())
        .arg("--progress")
        .arg(config.progress.as_str())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    // Run beside the binary so it discovers its own `.env` by walking up from
    // its directory, exactly as it would when invoked directly.
    if let Some(directory) = binary
        .parent()
        .filter(|directory| !directory.as_os_str().is_empty() && directory.is_dir())
    {
        command.current_dir(directory);
    }
    if let Some(model) = model {
        command.arg("--model").arg(model);
    }
    if let Some(instructions) = defaults.instructions.as_deref() {
        command.arg("--instructions").arg(instructions);
    }
    if let Some(prompt) = config.system_prompt.as_deref() {
        command.arg("--system-prompt").arg(prompt);
    }
    if let Some(file) = config.system_prompt_file.as_deref() {
        command.arg("--system-prompt-file").arg(file);
    }
    if config.require_parameter_support {
        command.arg("--require-parameter-support");
    }
    if let Some(tokens) = config.max_output_tokens {
        command.arg("--max-output-tokens").arg(tokens.to_string());
    }
    if let Some(temperature) = config.temperature {
        command.arg("--temperature").arg(temperature.to_string());
    }
    for provider in &config.allowed_providers {
        command.arg("--allow-provider").arg(provider);
    }
    // Pass a Koharu-managed credential when one is configured, otherwise leave
    // the variable unset so the binary can read its own `.env`/environment.
    if config.provider == "openrouter"
        && let Some(key) = koharu_secrets::get("openrouter")?
        && !key.expose_secret().trim().is_empty()
    {
        command.env("OPENROUTER_API_KEY", key.expose_secret());
    }

    let mut child = command.spawn().with_context(|| {
        format!(
            "failed to launch {}; set [full_context].binary_path or JSON_PAGE_TRANSLATE_BIN, or put {} on PATH",
            binary.display(),
            EXECUTABLE
        )
    })?;
    let stderr = child
        .stderr
        .take()
        .context("json-page-translate did not expose stderr")?;

    let log = Arc::new(Mutex::new(Vec::<String>::new()));
    let failed = Arc::new(Mutex::new(Vec::<u32>::new()));
    let debug_log = open_debug_log(config, &binary);
    let reader = spawn_stderr_reader(
        stderr,
        Arc::clone(&log),
        Arc::clone(&failed),
        page_map.clone(),
        progress,
        debug_log,
    );

    let status = tokio::select! {
        status = child.wait() => Some(status?),
        () = wait_for_stop(stop) => {
            let _ = child.start_kill();
            let _ = child.wait().await;
            None
        }
    };
    let _ = reader.await;

    let Some(status) = status else {
        return Ok(TranslationRun {
            stopped: true,
            ..TranslationRun::default()
        });
    };

    let code = status.code();
    match code {
        Some(0) | Some(2) => {}
        Some(other) => {
            let detail = log.lock().join("\n");
            bail!(
                "json-page-translate exited with code {other}{}",
                if detail.is_empty() {
                    String::new()
                } else {
                    format!(":\n{detail}")
                }
            );
        }
        None => {
            return Ok(TranslationRun {
                stopped: true,
                ..TranslationRun::default()
            });
        }
    }

    let output = match tokio::fs::read(&output_path).await {
        Ok(bytes) => Some(
            serde_json::from_slice::<TextExport>(&bytes)
                .context("json-page-translate produced an invalid document")?,
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error).context("failed to read the translation output"),
    };

    let failed_pages = failed.lock().clone();
    Ok(TranslationRun {
        output,
        failed_pages,
        stopped: false,
    })
}

/// Opens the JSONL transcript every preview event is appended to. It lives in
/// the current working directory (the workspace during development) so it is
/// easy to find and survives a stopped or failed run.
fn open_debug_log(
    config: &FullContextConfig,
    binary: &Path,
) -> Option<Arc<Mutex<std::fs::File>>> {
    use std::io::Write as _;

    if config.progress != ProgressMode::Preview {
        return None;
    }
    let path = debug_log_path(binary)?;
    match std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&path)
    {
        Ok(mut file) => {
            let _ = writeln!(file, "# json-page-translate event transcript");
            tracing::info!(
                target: "koharu::full_context",
                metric = "full_context_debug_log",
                path = %path.display(),
            );
            Some(Arc::new(Mutex::new(file)))
        }
        Err(error) => {
            tracing::warn!(
                target: "koharu::full_context",
                %error,
                path = %path.display(),
                "failed to open the full-context debug transcript",
            );
            None
        }
    }
}

/// The transcript lives next to the translator itself (its own repository
/// during development). It must never be written inside the application's
/// source tree: a dev file watcher rebuilds and restarts the app on every
/// write, which would restart the run in a loop.
fn debug_log_path(binary: &Path) -> Option<PathBuf> {
    let directory = binary
        .parent()
        .filter(|directory| !directory.as_os_str().is_empty())?;
    let profile = directory.file_name()?.to_str()?;
    if profile == "release" || profile == "debug" {
        if let Some(repo) = directory.parent().and_then(Path::parent) {
            return Some(repo.join(DEBUG_LOG_FILE));
        }
    }
    Some(directory.join(DEBUG_LOG_FILE))
}

/// Adapts in-process pipeline progress into the job-facing reporter.
fn pipeline_progress(progress: &ProgressReporter) -> ProgressSink {
    let progress = Arc::clone(progress);
    let state = Arc::new(Mutex::new((0_usize, 0_usize)));
    Arc::new(move |event: Progress| {
        let update = match event {
            Progress::Started { pages, stages } => {
                let total = pages.len().saturating_mul(stages.len());
                *state.lock() = (0, total);
                Some((0, total, None, None, None))
            }
            Progress::Loading { page, stage, model } => {
                let (completed, total) = *state.lock();
                Some((completed, total, Some(page), Some(stage), Some(model)))
            }
            Progress::Running { stage, model, .. } => {
                let (completed, total) = *state.lock();
                Some((completed, total, None, Some(stage), Some(model)))
            }
            Progress::Finished { page, stage, model, .. } => {
                let mut state = state.lock();
                state.0 = state.0.saturating_add(1).min(state.1);
                Some((state.0, state.1, Some(page), Some(stage), Some(model)))
            }
            Progress::Skipped { page, stage } => {
                let mut state = state.lock();
                state.0 = state.0.saturating_add(1).min(state.1);
                Some((state.0, state.1, Some(page), Some(stage), None))
            }
        };
        if let Some((completed, total, page, stage, model)) = update {
            progress(FullContextProgress::Pipeline {
                completed: Some(completed),
                total: Some(total),
                page,
                stage,
                model,
            });
        }
    })
}

fn spawn_stderr_reader(
    stderr: tokio::process::ChildStderr,
    log: Arc<Mutex<Vec<String>>>,
    failed: Arc<Mutex<Vec<u32>>>,
    page_map: BTreeMap<u32, EntityId>,
    progress: ProgressReporter,
    debug_log: Option<Arc<Mutex<std::fs::File>>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // Distinct pages parsed so far (from reasoning or the answer). Drives
        // the progress bar and resets when the translator retries a batch.
        let mut parsed: BTreeSet<u32> = BTreeSet::new();
        let mut total = 0_usize;
        let mut lines = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if let Some(file) = debug_log.as_ref() {
                use std::io::Write as _;
                if let Some(mut file) = file.try_lock() {
                    let _ = writeln!(file, "{line}");
                }
            }
            {
                let mut log = log.lock();
                if log.len() >= 64 {
                    log.remove(0);
                }
                log.push(line.clone());
            }
            let Ok(value) = serde_json::from_str::<Value>(&line) else {
                continue;
            };
            match value.get("event").and_then(Value::as_str) {
                Some("start") => {
                    total = value
                        .get("pages")
                        .and_then(Value::as_u64)
                        .unwrap_or(0) as usize;
                    progress(FullContextProgress::Total { total });
                }
                Some("page") => {
                    let number = value.get("page").and_then(Value::as_u64).unwrap_or(0) as u32;
                    let status = value.get("status").and_then(Value::as_str).unwrap_or("");
                    let attempt = value
                        .get("attempt")
                        .and_then(Value::as_u64)
                        .unwrap_or(1);
                    let source = value
                        .get("source")
                        .and_then(Value::as_str)
                        .map(str::to_owned);
                    if status == "streaming" {
                        let label = source.as_deref().unwrap_or("unknown");
                        tracing::info!(
                            target: "koharu::full_context",
                            metric = "full_context_page",
                            page = number,
                            attempt,
                            source = label,
                        );
                    }
                    parsed.insert(number);
                    progress(FullContextProgress::Page {
                        completed: parsed.len(),
                        total,
                        page: page_map.get(&number).copied(),
                        source,
                    });
                }
                Some("reasoning") => {
                    let text = value.get("text").and_then(Value::as_str).unwrap_or_default();
                    tracing::info!(
                        target: "koharu::full_context",
                        metric = "full_context_reasoning",
                        text,
                    );
                }
                Some("content") => {
                    let text = value.get("text").and_then(Value::as_str).unwrap_or_default();
                    tracing::info!(
                        target: "koharu::full_context",
                        metric = "full_context_content",
                        text,
                    );
                }
                Some("batch") => {
                    let attempt = value.get("attempt").and_then(Value::as_u64).unwrap_or(1);
                    if attempt > 1 {
                        // The previous attempt's parsing is void.
                        parsed.clear();
                    }
                    let pages = value
                        .get("pages")
                        .and_then(Value::as_array)
                        .map(|pages| {
                            pages
                                .iter()
                                .filter_map(Value::as_u64)
                                .map(|page| page as u32)
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default();
                    progress(FullContextProgress::Batch { pages });
                }
                Some("request") => {
                    let request = value.get("request").cloned().unwrap_or(Value::Null);
                    tracing::info!(
                        target: "koharu::full_context",
                        metric = "full_context_request",
                        request = %request,
                    );
                }
                Some("response") => {
                    let cost = value.get("cost").and_then(Value::as_f64);
                    let model = value
                        .get("model")
                        .and_then(Value::as_str)
                        .map(str::to_owned);
                    let provider = value
                        .get("provider")
                        .and_then(Value::as_str)
                        .map(str::to_owned);
                    let usage = value.get("usage").cloned().unwrap_or(Value::Null);
                    let finish_reason = value
                        .get("finish_reason")
                        .and_then(Value::as_str)
                        .map(str::to_owned);
                    let output = value
                        .get("content")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned();
                    tracing::info!(
                        target: "koharu::full_context",
                        metric = "full_context_response",
                        model,
                        provider,
                        cost,
                        finish_reason,
                        usage = %usage,
                        output = %output,
                    );
                    progress(FullContextProgress::Response {
                        cost,
                        model,
                        provider,
                    });
                }
                Some("done") => {
                    let pages = value
                        .get("failed")
                        .and_then(Value::as_array)
                        .map(|pages| {
                            pages
                                .iter()
                                .filter_map(Value::as_u64)
                                .map(|page| page as u32)
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default();
                    *failed.lock() = pages.clone();
                    progress(FullContextProgress::Done { failed: pages });
                }
                _ => {}
            }
        }
    })
}

async fn wait_for_stop(stop: &StopToken) {
    while !stop.stopped() {
        tokio::time::sleep(STOP_POLL).await;
    }
}

async fn import_translations(handle: &AppHandle<CefRuntime>, output: &TextExport) -> Result<usize> {
    let preferences = Preferences::load()?;
    let language = LanguageTag::new(preferences.pipeline.translation.target_language.tag())?;
    let generation = Generation {
        producer: ProducerId::new(PRODUCER)?,
        model: None,
        confidence: None,
    };

    let desktop = handle.state::<Desktop>();
    let (commit, page, applied) = {
        let current = handle.state::<CurrentProject>();
        let mut current = current.project.lock().await;
        let project = current.as_mut().context("no project is open")?;
        let (commit, result) = super::output::import_text_export(
            project,
            output,
            TextExportKind::Translation,
            TextExportOrigin::Generated {
                generation,
                language: Some(language),
            },
        )
        .await?;
        (commit, project.active_page(), result.applied as usize)
    };
    if let Some(commit) = commit {
        desktop
            .synchronize(&commit.snapshot, page, &commit)
            .await?;
        let canvas = desktop.canvas_state();
        handle.state::<CanvasChannel>().channel.publish(canvas);
    }
    Ok(applied)
}

fn resolve_binary(handle: &AppHandle<CefRuntime>, configured: Option<&str>) -> PathBuf {
    if let Some(path) = configured
        && !path.trim().is_empty()
    {
        return PathBuf::from(path);
    }
    if let Some(path) = std::env::var_os("JSON_PAGE_TRANSLATE_BIN") {
        return PathBuf::from(path);
    }
    let names: &[&str] = if cfg!(windows) {
        &["json-page-translate.exe", "json-page-translate"]
    } else {
        &["json-page-translate"]
    };
    if let Ok(executable) = std::env::current_exe()
        && let Some(directory) = executable.parent()
        && let Some(path) = find_binary(directory, names)
    {
        return path;
    }
    // Development convenience: a sibling `json-page-translate` checkout next to
    // (or above) the running executable, built with `cargo build --release`.
    if let Ok(executable) = std::env::current_exe()
        && let Some(path) = sibling_binary(&executable, names[0])
    {
        return path;
    }
    if let Ok(resource) = handle.path().resource_dir()
        && let Some(path) = find_binary(&resource, names)
    {
        return path;
    }
    PathBuf::from(EXECUTABLE)
}

fn sibling_binary(executable: &Path, name: &str) -> Option<PathBuf> {
    let mut ancestor = executable.parent();
    while let Some(directory) = ancestor {
        let candidate = directory
            .join(EXECUTABLE)
            .join("target")
            .join("release")
            .join(name);
        if candidate.is_file() {
            return Some(candidate);
        }
        ancestor = directory.parent();
    }
    None
}

fn find_binary(directory: &Path, names: &[&str]) -> Option<PathBuf> {
    names
        .iter()
        .map(|name| directory.join(name))
        .find(|candidate| candidate.is_file())
}

async fn check_protocol(binary: &Path) {
    match tokio::process::Command::new(binary)
        .arg("--protocol-version")
        .output()
        .await
    {
        Ok(output) if output.status.success() => {
            let version = String::from_utf8_lossy(&output.stdout).trim().to_owned();
            if version != PROTOCOL_VERSION {
                tracing::warn!(
                    %version,
                    expected = PROTOCOL_VERSION,
                    "json-page-translate protocol version mismatch"
                );
            }
        }
        Ok(output) => tracing::warn!(
            status = ?output.status,
            "failed to read the json-page-translate protocol version"
        ),
        Err(error) => tracing::warn!(
            %error,
            "failed to launch json-page-translate for a protocol version check"
        ),
    }
}

struct FullContextCommitter {
    handle: AppHandle<CefRuntime>,
}

#[async_trait::async_trait]
impl Committer for FullContextCommitter {
    async fn commit(&mut self, output: StageOutput) -> Result<Snapshot> {
        let (commit, page) = {
            let current = self.handle.state::<CurrentProject>();
            let mut current = current.project.lock().await;
            let project = current.as_mut().context("no project is open")?;
            let Some(commit) = project.commit_rebased(output.patch).await? else {
                return Ok(project.snapshot());
            };
            project.record_commit(&commit);
            (commit, project.active_page())
        };
        let snapshot = commit.snapshot.clone();
        let desktop = self.handle.state::<Desktop>();
        desktop
            .synchronize(&commit.snapshot, page, &commit)
            .await?;
        let canvas = desktop.canvas_state();
        self.handle
            .state::<CanvasChannel>()
            .channel
            .publish(canvas);
        Ok(snapshot)
    }
}

#[tauri::command]
#[specta::specta]
pub(crate) async fn full_context_translate(
    handle: AppHandle<CefRuntime>,
    processing: State<'_, Processing>,
    job_channel: State<'_, JobChannel>,
) -> std::result::Result<JobId, Error> {
    let id = JobId::new();
    let stop = StopToken::default();
    {
        let mut stops = processing.stops.lock();
        if !stops.is_empty() {
            return Err(anyhow!("another process is already running").into());
        }
        stops.insert(id, stop.clone());
    }
    let job = Job {
        id,
        state: JobState::Running,
        completed: 0,
        total: 0,
        page: None,
        stage: None,
        model: None,
        detail: None,
        source: None,
        cost: None,
        error: None,
    };
    processing.jobs.lock().insert(id, job.clone());
    job_channel.channel.publish(job);

    let task_handle = handle.clone();
    drop(tauri::async_runtime::spawn(async move {
        let progress_handle = task_handle.clone();
        let progress: ProgressReporter = Arc::new(move |event| {
            let processing = progress_handle.state::<Processing>();
            let mut jobs = processing.jobs.lock();
            let Some(job) = jobs.get_mut(&id) else {
                return;
            };
            match event {
                FullContextProgress::Total { total } => {
                    job.total = total;
                    job.stage = Some(Stage::Translation);
                }
                FullContextProgress::Page {
                    completed,
                    total,
                    page,
                    source,
                } => {
                    job.completed = completed;
                    if total > 0 {
                        job.total = total;
                    }
                    job.page = page;
                    if let Some(source) = source {
                        job.source = Some(source);
                    }
                    job.stage = Some(Stage::Translation);
                }
                FullContextProgress::Pipeline {
                    completed,
                    total,
                    page,
                    stage,
                    model,
                } => {
                    if let Some(completed) = completed {
                        job.completed = completed;
                    }
                    if let Some(total) = total {
                        job.total = total;
                    }
                    if let Some(page) = page {
                        job.page = Some(page);
                    }
                    job.stage = stage.or(job.stage);
                    if let Some(model) = model {
                        job.model = Some(model);
                    }
                }
                FullContextProgress::Batch { pages } => {
                    job.detail = Some(
                        pages
                            .iter()
                            .map(u32::to_string)
                            .collect::<Vec<_>>()
                            .join(", "),
                    );
                    job.stage = Some(Stage::Translation);
                }
                FullContextProgress::Response {
                    cost,
                    model,
                    provider,
                } => {
                    if let Some(cost) = cost {
                        job.cost = Some(job.cost.unwrap_or(0.0) + cost);
                    }
                    job.model = model.or(provider).or_else(|| job.model.take());
                }
                FullContextProgress::Done { .. } => {}
            }
            let job = job.clone();
            drop(jobs);
            progress_handle
                .state::<JobChannel>()
                .channel
                .publish(job);
        });

        let result = execute(&task_handle, stop.clone(), progress).await;
        let (stopped, error, failed) = match result {
            Ok(outcome) => (outcome.stopped, None, outcome.failed_pages),
            Err(error) => (stop.stopped(), Some(format!("{error:#}")), Vec::new()),
        };
        if !failed.is_empty() {
            tracing::warn!(?failed, "whole-work translation left pages untranslated");
        }

        task_handle.state::<Processing>().stops.lock().remove(&id);
        let job = task_handle
            .state::<Processing>()
            .jobs
            .lock()
            .remove(&id)
            .map(|mut job| {
                job.state = if stopped {
                    JobState::Stopped
                } else if error.is_some() {
                    JobState::Failed
                } else {
                    JobState::Finished
                };
                job.error = error;
                job
            });
        if let Some(job) = job {
            task_handle.state::<JobChannel>().channel.publish(job);
        }
    }));
    Ok(id)
}

#[cfg(test)]
mod tests {
    use koharu_scene::{
        At, Authored, Origin, PageDraft, SourceText, TextLayout, TextLayoutKind,
    };

    use super::*;
    use crate::commands::project::Project;

    async fn session() -> koharu_scene::Session {
        let mut session = koharu_scene::Session::memory().await.unwrap();
        let mut edit = session.snapshot().edit();
        let first = edit
            .add_page(PageDraft::new("first", 100.0, 100.0), At::End)
            .unwrap();
        let second = edit
            .add_page(PageDraft::new("second", 100.0, 100.0), At::End)
            .unwrap();
        for (page, text) in [(first, "hello"), (second, "")] {
            let content = edit.add_text_content(page, At::End).unwrap();
            edit.set(
                content,
                &SourceText {
                    text: Authored::user(text.to_owned()),
                    language: None,
                },
            )
            .unwrap();
            edit.add_text_layer(
                page,
                At::End,
                content,
                &TextLayout {
                    origin: Origin::User,
                    kind: TextLayoutKind::Paragraph,
                    angle_degrees: None,
                },
            )
            .unwrap();
            let blank = edit.add_text_content(page, At::End).unwrap();
            edit.set(
                blank,
                &SourceText {
                    text: Authored::user(String::new()),
                    language: None,
                },
            )
            .unwrap();
            edit.add_text_layer(
                page,
                At::End,
                blank,
                &TextLayout {
                    origin: Origin::User,
                    kind: TextLayoutKind::Paragraph,
                    angle_degrees: None,
                },
            )
            .unwrap();
        }
        session.commit(edit.finish().unwrap()).await.unwrap();
        session
    }

    async fn scene() -> Snapshot {
        session().await.snapshot()
    }

    fn first_layer(snapshot: &Snapshot) -> EntityId {
        snapshot
            .pages()
            .next()
            .unwrap()
            .text_group()
            .unwrap()
            .unwrap()
            .text_layers()
            .unwrap()
            .next()
            .unwrap()
            .id()
    }

    #[tokio::test]
    async fn document_covers_every_page_in_order() {
        let snapshot = scene().await;
        let (document, page_ids) = build_document(&snapshot).unwrap();

        assert_eq!(document.pages.len(), 2);
        assert_eq!(document.pages[0].page, 1);
        assert_eq!(document.pages[0].texts, vec!["hello", ""]);
        assert_eq!(document.pages[1].page, 2);
        assert_eq!(document.pages[1].texts, vec!["", ""]);
        let first = snapshot.pages().next().unwrap().id();
        assert_eq!(page_ids.get(&1), Some(&first));
    }

    #[tokio::test]
    async fn generated_import_writes_content_and_preserves_user_text() {
        let mut project = Project::new(session().await, "test".to_owned());
        let language = LanguageTag::new("fr-FR").unwrap();
        let generation = Generation {
            producer: ProducerId::new(PRODUCER).unwrap(),
            model: None,
            confidence: None,
        };
        let export = TextExport {
            pages: vec![TextExportPage {
                page: 1,
                texts: vec!["bonjour".to_owned(), String::new()],
            }],
        };

        let (commit, result) = crate::commands::output::import_text_export(
            &mut project,
            &export,
            TextExportKind::Translation,
            TextExportOrigin::Generated {
                generation: generation.clone(),
                language: Some(language.clone()),
            },
        )
        .await
        .unwrap();
        assert!(commit.is_some());
        assert_eq!(result.applied, 1);

        let layer = first_layer(&project.snapshot());
        let translation = project
            .snapshot()
            .text_layer(layer)
            .unwrap()
            .content()
            .unwrap()
            .translation()
            .unwrap()
            .unwrap();
        assert_eq!(translation.text.value, "bonjour");
        assert!(matches!(translation.text.origin, Origin::Generated(_)));

        project
            .set_translation(layer, Some("écrit".to_owned()))
            .await
            .unwrap();
        let (commit, _) = crate::commands::output::import_text_export(
            &mut project,
            &export,
            TextExportKind::Translation,
            TextExportOrigin::Generated {
                generation,
                language: Some(language),
            },
        )
        .await
        .unwrap();
        assert!(commit.is_none());
        assert_eq!(
            project
                .snapshot()
                .text_layer(layer)
                .unwrap()
                .content()
                .unwrap()
                .translation()
                .unwrap()
                .unwrap()
                .text
                .value,
            "écrit"
        );
    }
}
