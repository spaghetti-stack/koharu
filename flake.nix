{
  description = "Koharu dev shell";
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };
  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          config.allowUnfree = true;
          config.cudaSupport = true;
        };

        animeAceFonts = pkgs.stdenv.mkDerivation {
          pname = "anime-ace-fonts";
          version = "1.0";
          src = ./fonts;
          dontBuild = true;
          installPhase = ''
            runHook preInstall
            mkdir -p $out/share/fonts/anime-ace
            cp animeace.ttf      $out/share/fonts/anime-ace/
            cp animeace_i.ttf    $out/share/fonts/anime-ace/
            cp animeace_b.ttf    $out/share/fonts/anime-ace/
            runHook postInstall
          '';
        };
      in {
        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            bun
            nodejs
            pkg-config
            cmake
            uv
            python3

            at-spi2-atk
            atk
            atkmm
            cairo
            alsa-lib
            cups
            dbus
            fontconfig
            gdk-pixbuf
            glib
            gobject-introspection
            gtk3
            harfbuzz
            librsvg
            libuuid
            libsoup_3
            openssl
            pango
            webkitgtk_4_1
            xdg-utils
            zenity

            mesa
            libgbm
            libGL
            libxkbcommon
            nspr
            nss
            systemd
            vulkan-loader
            xorg.libX11
            xorg.libXcomposite
            xorg.libXdamage
            xorg.libXext
            xorg.libXfixes
            xorg.libXrandr
            xorg.libxcb

            cudaPackages.cudatoolkit

            animeAceFonts
          ];
          inputsFrom = [ pkgs.stdenv ];
          shellHook = ''
            export CUDA_PATH=${pkgs.cudatoolkit}
            export UV_PYTHON=${pkgs.python3}/bin/python3
            # On NixOS, the NVIDIA driver is bind-mounted at /run/opengl-driver
            # by the system config (hardware.nvidia). Prepending it to
            # LD_LIBRARY_PATH exposes libcuda.so.1, libnvidia-*.so, etc. so
            # the runtime's dlopen("libcuda.so.1") and llama.cpp's CUDA loader
            # can find them. The packaged koharu (buildFHSEnv) gets the same
            # mount via the FHS env.
            export LD_LIBRARY_PATH=${pkgs.cudatoolkit}/lib:${pkgs.llvmPackages.libclang.lib}/lib:${pkgs.gcc.cc.lib}/lib:${pkgs.vulkan-loader}/lib:${pkgs.mesa}/lib:${pkgs.libgbm}/lib:${pkgs.nspr}/lib:${pkgs.nss}/lib:${pkgs.at-spi2-atk}/lib:${pkgs.atk}/lib:${pkgs.atkmm}/lib:${pkgs.alsa-lib}/lib:${pkgs.cairo}/lib:${pkgs.cups.lib}/lib:${pkgs.dbus.lib}/lib:${pkgs.fontconfig}/lib:${pkgs.gdk-pixbuf}/lib:${pkgs.glib.out}/lib:${pkgs.gtk3}/lib:${pkgs.harfbuzz}/lib:${pkgs.librsvg}/lib:${pkgs.libuuid}/lib:${pkgs.libsoup_3}/lib:${pkgs.pango.out}/lib:${pkgs.libxkbcommon}/lib:${pkgs.systemd}/lib:${pkgs.xorg.libX11}/lib:${pkgs.xorg.libXcomposite}/lib:${pkgs.xorg.libXdamage}/lib:${pkgs.xorg.libXext}/lib:${pkgs.xorg.libXfixes}/lib:${pkgs.xorg.libXrandr}/lib:${pkgs.xorg.libxcb}/lib''${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}
            [ -d /run/opengl-driver/lib ] && export LD_LIBRARY_PATH=/run/opengl-driver/lib:$LD_LIBRARY_PATH
            # CEF/ANGLE can fail to choose an adapter when the Nix Mesa ICDs
            # and the host NVIDIA ICD are both visible. Prefer the host NVIDIA
            # ICD when available, while allowing an explicit user override.
            if [ -z "''${VK_ICD_FILENAMES:-}" ] && [ -f /run/opengl-driver/share/vulkan/icd.d/nvidia_icd.json ]; then
              export VK_ICD_FILENAMES=/run/opengl-driver/share/vulkan/icd.d/nvidia_icd.json
            fi
            export LIBCLANG_PATH=${pkgs.llvmPackages.libclang.lib}/lib
            export BINDGEN_EXTRA_CLANG_ARGS="-isystem $(echo -n ${pkgs.stdenv.cc.cc}/include/c++) -isystem $(echo -n ${pkgs.stdenv.cc.cc}/include) -isystem $(echo -n ${pkgs.glibc.dev}/include)"
            export GSK_RENDERER=cairo
            export WEBKIT_DISABLE_DMABUF_RENDERER=1
            # WebKitGTK 4.1 webview crashes with SIGABRT on NixOS without these.
            # Forces CPU compositing and disables the DMA-BUF renderer path that
            # tries to use GBM/EGL directly (which fails on NixOS).
            export WEBKIT_DISABLE_COMPOSITING_MODE=1
            # Make XDG-aware tools see the dev shell's /share (fonts, icons, etc.)
            export XDG_DATA_DIRS=${pkgs.gtk3}/share:${pkgs.librsvg}/share:${pkgs.harfbuzz}/share:''${XDG_DATA_DIRS:+:$XDG_DATA_DIRS}

            # Make fontconfig scan the anime-ace fonts shipped with the dev
            # shell while still exposing the host's system fonts. Setting
            # FONTCONFIG_FILE replaces the default config entirely, so without
            # the include below the app only sees anime-ace and loses Noto,
            # DejaVu, ~/.local/share/fonts, etc.
            export FONTCONFIG_FILE=${pkgs.writeText "anime-ace-fonts.conf" ''
              <?xml version="1.0"?>
              <!DOCTYPE fontconfig SYSTEM "urn:fontconfig:fonts.dtd">
              <fontconfig>
                <include ignore_missing="yes">/etc/fonts/fonts.conf</include>
                <dir>${animeAceFonts}/share/fonts</dir>
              </fontconfig>
            ''}
          '';
        };
      });
}
