use anyhow::{Context as _, Result};
use futures::future::try_join_all;
use image::{
    ExtendedColorType, ImageEncoder as _,
    codecs::png::{CompressionType, FilterType, PngEncoder},
};
use koharu_psd::{PsdExportOptions, export_page};
use koharu_rasterizer::{Raster, RasterOptions, Rasterizer};
use koharu_renderer::{Frame, Renderer};
use koharu_scene::{AssetRole, Commit, EntityId, Generation, LanguageTag, Snapshot};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use specta::Type;
use std::{io::Write as _, sync::Arc};
use tauri::{State, WebviewWindow, ipc::IpcResponse};
use tauri_runtime_cef::CefRuntime;

use super::{
    ChannelExt as _, Error,
    canvas::CanvasChannel,
    project::{CurrentProject, Project},
};
use koharu_desktop::Desktop;

const THUMBNAIL_EDGE: u32 = 128;

#[derive(Type)]
#[specta(transparent)]
pub(crate) struct ThumbnailBytes(#[specta(type = Vec<u8>)] Vec<u8>);

impl IpcResponse for ThumbnailBytes {
    fn body(self) -> tauri::Result<tauri::ipc::InvokeResponseBody> {
        Ok(self.0.into())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Type)]
#[serde(rename_all = "snake_case")]
pub enum ExportFormat {
    Png,
    Psd,
    Cbz,
}

#[derive(Clone, Copy, Debug, Deserialize, Type)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TextExportKind {
    Source,
    Translation,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct TextExport {
    pub(crate) pages: Vec<TextExportPage>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct TextExportPage {
    pub(crate) page: usize,
    pub(crate) texts: Vec<String>,
}

/// Attribution applied to imported text.
#[derive(Clone)]
pub(crate) enum TextExportOrigin {
    /// The caller authored the text (manual file import).
    User,
    /// A machine produced the text. User-authored translations are preserved.
    Generated {
        generation: Generation,
        language: Option<LanguageTag>,
    },
}

#[derive(Debug, Serialize, Type)]
pub(crate) struct ImportTextsResult {
    pub applied: u32,
    pub skipped: Vec<ImportTextsSkip>,
    pub errors: Vec<String>,
}

#[derive(Debug, Serialize, Type)]
pub(crate) struct ImportTextsSkip {
    pub page: u32,
    pub reason: String,
}

fn serialize_text_export(pages: Vec<TextExportPage>) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec_pretty(&TextExport { pages })?)
}

fn text_export_page(page: usize, texts: Vec<String>) -> Option<TextExportPage> {
    texts
        .iter()
        .any(|text| !text.trim().is_empty())
        .then_some(TextExportPage { page, texts })
}

#[tauri::command]
#[specta::specta]
pub(crate) async fn export_texts(
    window: WebviewWindow<CefRuntime>,
    pages: Vec<EntityId>,
    project: State<'_, CurrentProject>,
    export_kind: TextExportKind,
) -> Result<(), Error> {
    let snapshot = {
        let project = project.project.lock().await;
        let project = project.as_ref().context("no project is open")?;
        project.snapshot()
    };
    let Some(file) = rfd::AsyncFileDialog::new()
        .set_parent(&window)
        .set_file_name(match export_kind {
            TextExportKind::Source => "source-texts.json",
            TextExportKind::Translation => "translations.json",
        })
        .save_file()
        .await
    else {
        return Ok(());
    };
    let pages = if pages.is_empty() {
        snapshot.pages().map(|page| page.id()).collect()
    } else {
        pages
    };

    if pages.is_empty() {
        return Err(anyhow::anyhow!("there are no pages to export").into());
    }

    let mut exported_pages = Vec::with_capacity(pages.len());
    for (page_index, page_id) in pages.into_iter().enumerate() {
        let page = snapshot.page(page_id)?;
        let mut texts = Vec::new();

        if let Some(text_group) = page.text_group()? {
            for layer in text_group.text_layers()? {
                let content = layer.content()?;
                let Some(source) = content.source()? else {
                    let text = match export_kind {
                        TextExportKind::Source => String::new(),
                        TextExportKind::Translation => content
                            .translation()?
                            .map_or_else(String::new, |text| text.text.value),
                    };
                    texts.push(text);
                    continue;
                };
                let text = match export_kind {
                    TextExportKind::Source => source.text.value,
                    TextExportKind::Translation => content
                        .translation()?
                        .map_or_else(String::new, |text| text.text.value),
                };
                texts.push(text);
            }
        }

        if let Some(page) = text_export_page(page_index + 1, texts) {
            exported_pages.push(page);
        }
    }

    let bytes = serialize_text_export(exported_pages)?;
    tokio::fs::write(file.path(), bytes).await?;
    Ok(())
}

#[tauri::command]
#[specta::specta]
pub(crate) async fn import_texts(
    window: WebviewWindow<CefRuntime>,
    project: State<'_, CurrentProject>,
    desktop: State<'_, Desktop>,
    canvas_channel: State<'_, CanvasChannel>,
    import_kind: TextExportKind,
) -> Result<ImportTextsResult, Error> {
    let Some(file) = rfd::AsyncFileDialog::new()
        .set_parent(&window)
        .add_filter("JSON", &["json"])
        .pick_file()
        .await
    else {
        return Ok(ImportTextsResult {
            applied: 0,
            skipped: Vec::new(),
            errors: Vec::new(),
        });
    };

    let bytes = tokio::fs::read(file.path()).await?;
    let input = String::from_utf8(bytes).context("the selected file is not valid UTF-8")?;
    let export: TextExport = match koharu_translator::parse_json(&input) {
        Ok(export) => export,
        Err(error) => {
            return Ok(ImportTextsResult {
                applied: 0,
                skipped: Vec::new(),
                errors: vec![format!("failed to parse JSON: {error}")],
            });
        }
    };

    let (commit, page, result) = {
        let mut project = project.project.lock().await;
        let project = project.as_mut().context("no project is open")?;
        let (commit, result) =
            import_text_export(project, &export, import_kind, TextExportOrigin::User).await?;
        (commit, project.active_page(), result)
    };

    if let Some(commit) = commit {
        desktop.synchronize(&commit.snapshot, page, &commit).await?;
        canvas_channel.channel.publish(desktop.canvas_state());
    }
    Ok(result)
}

/// Applies a parsed text document to every matching project page.
///
/// Page numbers are 1-based positions over `snapshot.pages()`; a page is
/// skipped when its text-layer count does not match. Shared by the manual
/// import command and whole-work translation.
pub(crate) async fn import_text_export(
    project: &mut Project,
    export: &TextExport,
    import_kind: TextExportKind,
    origin: TextExportOrigin,
) -> Result<(Option<Commit>, ImportTextsResult)> {
    let snapshot = project.snapshot();
    let page_ids = snapshot.pages().map(|page| page.id()).collect::<Vec<_>>();
    // Machine translation returns `""` for text it left unchanged; a manual
    // document may legitimately clear a field, so only generated imports skip.
    let skip_empty = matches!(origin, TextExportOrigin::Generated { .. });
    let mut last_commit = None;
    let mut revisions = Vec::new();
    let mut applied = 0_u32;
    let mut skipped = Vec::new();

    for (page_index, page_id) in page_ids.into_iter().enumerate() {
        let page_number = page_index + 1;
        let page = snapshot.page(page_id)?;
        let mut text_layers = Vec::new();
        let group = match page.text_group() {
            Ok(group) => group,
            Err(error) => {
                skipped.push(ImportTextsSkip {
                    page: page_number as u32,
                    reason: format!("failed to read text group: {error}"),
                });
                continue;
            }
        };
        if let Some(group) = group {
            let layers = match group.text_layers() {
                Ok(layers) => layers,
                Err(error) => {
                    skipped.push(ImportTextsSkip {
                        page: page_number as u32,
                        reason: format!("failed to read text layers: {error}"),
                    });
                    continue;
                }
            };
            for layer in layers {
                text_layers.push(layer.id());
            }
        }

        if text_layers.is_empty() {
            continue;
        }
        let Some(page_export) = export.pages.iter().find(|page| page.page == page_number) else {
            continue;
        };
        if page_export.texts.len() != text_layers.len() {
            skipped.push(ImportTextsSkip {
                page: page_number as u32,
                reason: format!(
                    "text count mismatch: expected {}, got {}",
                    text_layers.len(),
                    page_export.texts.len()
                ),
            });
            continue;
        }

        for (layer, text) in text_layers.into_iter().zip(&page_export.texts) {
            if skip_empty && text.trim().is_empty() {
                continue;
            }
            let commit = match (import_kind, origin.clone()) {
                (TextExportKind::Source, _) => {
                    project.set_source_text(layer, text.clone()).await.map(Some)
                }
                (TextExportKind::Translation, TextExportOrigin::User) => project
                    .set_translation(layer, Some(text.clone()))
                    .await
                    .map(Some),
                (
                    TextExportKind::Translation,
                    TextExportOrigin::Generated {
                        generation,
                        language,
                    },
                ) => {
                    project
                        .set_translation_generated(layer, text.clone(), generation, language)
                        .await
                }
            };
            match commit {
                Ok(Some(commit)) => {
                    revisions.push(commit.revision);
                    last_commit = Some(commit);
                }
                Ok(None) => {}
                Err(error) => {
                    skipped.push(ImportTextsSkip {
                        page: page_number as u32,
                        reason: format!("failed to apply text: {error}"),
                    });
                    break;
                }
            }
        }
        applied += 1;
    }
    project.record(revisions);

    Ok((
        last_commit,
        ImportTextsResult {
            applied,
            skipped,
            errors: Vec::new(),
        },
    ))
}

#[tracing::instrument(
    target = "koharu_metrics",
    name = "export",
    skip_all,
    fields(origin = "user", format = ?format),
)]
#[tauri::command]
#[specta::specta]
pub(crate) async fn export(
    window: WebviewWindow<CefRuntime>,
    format: ExportFormat,
    project: State<'_, CurrentProject>,
    desktop: State<'_, Desktop>,
) -> std::result::Result<(), Error> {
    let (name, snapshot) = {
        let project = project.project.lock().await;
        let project = project.as_ref().context("no project is open")?;
        (project.name.clone(), project.snapshot())
    };
    let pages = snapshot.pages().map(|page| page.id()).collect::<Vec<_>>();
    if pages.is_empty() {
        return Err(anyhow::anyhow!("there are no pages to export").into());
    }
    let dialog = rfd::AsyncFileDialog::new().set_parent(&window);
    let destination = match format {
        ExportFormat::Png | ExportFormat::Psd => dialog.pick_folder().await,
        ExportFormat::Cbz => {
            dialog
                .add_filter("Comic Book Archive", &["cbz"])
                .set_file_name(format!("{name}.cbz"))
                .save_file()
                .await
        }
    };
    let Some(destination) = destination.map(|destination| destination.path().to_owned()) else {
        return Ok(());
    };
    let renderer = desktop.renderer();
    let rasterizer = desktop.rasterizer().await?;
    let frames = try_join_all(pages.iter().map(|&page| renderer.render(&snapshot, page))).await?;
    let (extension, images) = match format {
        ExportFormat::Png | ExportFormat::Cbz => {
            let images = tokio_rayon::spawn(move || {
                frames
                    .par_iter()
                    .map(|frame| -> Result<_> {
                        let image = rasterizer
                            .rasterize(&frame.raster_frame()?, RasterOptions::default())?
                            .image;
                        let mut bytes = Vec::new();
                        PngEncoder::new_with_quality(
                            &mut bytes,
                            CompressionType::Best,
                            FilterType::Adaptive,
                        )
                        .write_image(
                            image.as_raw(),
                            image.width(),
                            image.height(),
                            ExtendedColorType::Rgba8,
                        )?;
                        Ok(bytes)
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .await?;
            ("png", images)
        }
        ExportFormat::Psd => {
            let options = PsdExportOptions::default();
            let images = try_join_all(
                frames
                    .iter()
                    .map(|frame| export_page(Arc::clone(&rasterizer), &snapshot, frame, &options)),
            )
            .await?;
            ("psd", images)
        }
    };
    tokio_rayon::spawn(move || -> Result<()> {
        let mut archive = if matches!(format, ExportFormat::Cbz) {
            let directory = destination.parent().context("archive path has no parent")?;
            Some(zip::ZipWriter::new(tempfile::NamedTempFile::new_in(
                directory,
            )?))
        } else {
            None
        };
        let width = pages.len().to_string().len().max(4);
        // Async preparation and indexed encoding both preserve project order.
        for (index, (page_id, bytes)) in pages.into_iter().zip(images).enumerate() {
            let page = snapshot.page(page_id)?.page()?;
            let name = page
                .label
                .trim()
                .trim_end_matches(|character: char| character == '.' || character.is_whitespace());
            let name = name
                .rsplit_once('.')
                .map_or(name, |(stem, _)| stem)
                .replace(['<', '>', ':', '"', '/', '\\', '|', '?', '*'], "_");
            let name = format!(
                "{:0width$}_{}.{extension}",
                index + 1,
                if name.is_empty() { "page" } else { &name }
            );
            if let Some(archive) = &mut archive {
                // PNG data is already compressed.
                let options = zip::write::SimpleFileOptions::default()
                    .compression_method(zip::CompressionMethod::Stored);
                archive.start_file(name, options)?;
                archive.write_all(&bytes)?;
            } else {
                std::fs::write(destination.join(name), bytes)?;
            }
            tracing::info!(
                target: "koharu_metrics",
                metric = "page_exported",
                format = ?format,
            );
        }
        if let Some(archive) = archive {
            archive.finish()?.persist(destination)?;
        }
        Ok(())
    })
    .await?;
    Ok(())
}

#[tauri::command]
#[specta::specta]
pub(crate) async fn get_thumbnail(
    page: EntityId,
    project: State<'_, CurrentProject>,
) -> std::result::Result<ThumbnailBytes, Error> {
    let snapshot = project
        .project
        .lock()
        .await
        .as_ref()
        .context("no project is open")?
        .snapshot();
    snapshot.page(page)?;
    let blob = snapshot
        .asset(page, &AssetRole::new("source")?)?
        .with_context(|| format!("page {page} has no source image"))?
        .blob;
    let bytes = snapshot.read_blob(blob).await?;
    let bytes = tokio_rayon::spawn(move || -> Result<Vec<u8>> {
        let image = image::load_from_memory(&bytes).context("failed to decode source image")?;
        if image.width() == 0 || image.height() == 0 {
            return Err(anyhow::anyhow!("source image is empty"));
        }
        let image = image.thumbnail(THUMBNAIL_EDGE, THUMBNAIL_EDGE).to_rgba8();
        let encoder = webp::Encoder::from_rgba(image.as_raw(), image.width(), image.height());
        Ok(encoder.encode(80.0).to_vec())
    })
    .await?;
    Ok(ThumbnailBytes(bytes))
}

pub(crate) async fn rendered_preview(
    renderer: &Renderer,
    rasterizer: Arc<Rasterizer>,
    snapshot: &Snapshot,
    page: EntityId,
) -> Result<Vec<u8>> {
    snapshot.page(page)?;
    let frame = renderer.render(snapshot, page).await?;
    let image = rasterize(rasterizer, &frame, RasterOptions::default())
        .await?
        .image;
    tokio_rayon::spawn(move || {
        let image = image::DynamicImage::ImageRgba8(image)
            .resize(1024, 1024, image::imageops::FilterType::Lanczos3)
            .to_rgba8();
        let encoder = webp::Encoder::from_rgba(image.as_raw(), image.width(), image.height());
        Ok::<_, anyhow::Error>(encoder.encode(85.0).to_vec())
    })
    .await
}

async fn rasterize(
    rasterizer: Arc<Rasterizer>,
    frame: &Frame,
    options: RasterOptions,
) -> Result<Raster> {
    let frame = frame.raster_frame()?;
    tokio_rayon::spawn(move || rasterizer.rasterize(&frame, options))
        .await
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn omits_pages_with_only_empty_text() {
        assert!(text_export_page(2, vec![String::new(), "  ".to_owned()]).is_none());
    }

    #[test]
    fn preserves_empty_text_placeholders_on_non_empty_pages() {
        let page = text_export_page(
            1,
            vec!["first".to_owned(), String::new(), "third".to_owned()],
        )
        .expect("page contains text");

        assert_eq!(page.texts, vec!["first", "", "third"]);
    }

    #[test]
    fn serializes_empty_page_list_when_all_pages_are_empty() {
        let pages = [vec![String::new()], vec![" ".to_owned()]]
            .into_iter()
            .enumerate()
            .filter_map(|(index, texts)| text_export_page(index + 1, texts))
            .collect();
        let bytes = serialize_text_export(pages).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

        assert_eq!(value, serde_json::json!({ "pages": [] }));
    }

    #[test]
    fn exported_texts_round_trip_through_import_format() {
        let exported = TextExport {
            pages: vec![
                TextExportPage {
                    page: 1,
                    texts: vec!["First translation".to_owned(), String::new()],
                },
                TextExportPage {
                    page: 3,
                    texts: vec!["Only text on page 3".to_owned()],
                },
            ],
        };

        let bytes = serialize_text_export(exported.pages.clone()).unwrap();
        let imported: TextExport = serde_json::from_slice(&bytes).unwrap();

        assert_eq!(imported, exported);
    }
}
