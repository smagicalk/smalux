//! 前端资源模块，负责站点和管理后台的静态资源与 SPA fallback。

use std::path::Path;

use axum::Router;
use axum::routing::get;
#[cfg(feature = "frontend-embed")]
use axum::{
    body::Body,
    extract::Path as AxumPath,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use axum::{extract::Path as PlainPath, response::Redirect};
use tower_http::services::{ServeDir, ServeFile};

use crate::{
    config::model::{FrontendConfig, FrontendSlotConfig, FrontendSlotMode},
    state::AppState,
};

pub const SITE_ASSET_PATH: &str = "/assets/site";
pub const ADMIN_ASSET_PATH: &str = "/assets/admin";
pub const ADMIN_ROUTE_PATH: &str = "/admin";

pub fn build_frontend_router(config: &FrontendConfig) -> Router<AppState> {
    if !config.serve_frontend {
        tracing::debug!("frontend hosting disabled");
        return Router::new();
    }

    tracing::info!(
        site_dir = config
            .site
            .directory
            .as_ref()
            .map(|dir| dir.display().to_string())
            .as_deref()
            .unwrap_or("<embedded>"),
        admin_dir = config
            .admin
            .directory
            .as_ref()
            .map(|dir| dir.display().to_string())
            .as_deref()
            .unwrap_or("<embedded>"),
        spa_fallback = config.spa_fallback,
        "frontend slot routing prepared"
    );

    build_admin_router(config).merge(build_site_router(config))
}

fn build_admin_router(config: &FrontendConfig) -> Router<AppState> {
    build_slot_router(
        &config.admin,
        ADMIN_ROUTE_PATH,
        ADMIN_ASSET_PATH,
        FrontendSlot::Admin,
    )
}

fn build_site_router(config: &FrontendConfig) -> Router<AppState> {
    build_slot_router(&config.site, "/", SITE_ASSET_PATH, FrontendSlot::Site)
}

fn build_slot_router(
    slot: &FrontendSlotConfig,
    mount_path: &str,
    asset_path: &str,
    _embedded_slot: FrontendSlot,
) -> Router<AppState> {
    match slot.mode {
        FrontendSlotMode::Embedded => {
            #[cfg(feature = "frontend-embed")]
            {
                return build_embedded_slot_router(mount_path, asset_path, _embedded_slot);
            }

            #[cfg(not(feature = "frontend-embed"))]
            {
                return Router::new();
            }
        }
        FrontendSlotMode::Directory => {
            let Some(dir) = slot.directory.as_deref() else {
                return Router::new();
            };
            build_directory_slot_router(mount_path, asset_path, dir)
        }
        FrontendSlotMode::External => {
            let Some(external_url) = slot.external_url.as_deref() else {
                return Router::new();
            };
            build_external_slot_router(mount_path, asset_path, external_url)
        }
    }
}

fn build_directory_slot_router(
    mount_path: &str,
    asset_path: &str,
    base_dir: &Path,
) -> Router<AppState> {
    let index = base_dir.join("index.html");
    let page_router = Router::new()
        .route_service("/", ServeFile::new(index.clone()))
        .fallback_service(ServeFile::new(index));

    let page_router = if mount_path == "/" {
        page_router
    } else {
        Router::new().nest(mount_path, page_router)
    };

    page_router.nest_service(asset_path, ServeDir::new(base_dir.join("assets")))
}

fn build_external_slot_router(
    mount_path: &str,
    asset_path: &str,
    external_url: &str,
) -> Router<AppState> {
    let page_base = external_url.trim_end_matches('/').to_string();
    let asset_base = format!("{}/assets", page_base.trim_end_matches('/'));

    let page_router = Router::new()
        .route(
            "/",
            get({
                let page_base = page_base.clone();
                move || redirect_root_to(page_base.clone())
            }),
        )
        .route(
            "/{*path}",
            get({
                let page_base = page_base.clone();
                move |path| redirect_to(page_base.clone(), path)
            }),
        );

    let page_router = if mount_path == "/" {
        page_router
    } else {
        Router::new().nest(mount_path, page_router)
    };

    page_router.route(
        &format!("{asset_path}/{{*path}}"),
        get(move |path| redirect_to(asset_base.clone(), path)),
    )
}

async fn redirect_root_to(base: String) -> Redirect {
    Redirect::temporary(&base)
}

async fn redirect_to(base: String, PlainPath(path): PlainPath<String>) -> Redirect {
    if path.is_empty() {
        Redirect::temporary(&base)
    } else {
        Redirect::temporary(&format!("{}/{}", base.trim_end_matches('/'), path))
    }
}

#[derive(Clone, Copy)]
enum FrontendSlot {
    Site,
    Admin,
}

#[cfg(feature = "frontend-embed")]
fn build_embedded_slot_router(
    mount_path: &str,
    asset_path: &str,
    slot: FrontendSlot,
) -> Router<AppState> {
    let page_router = Router::new()
        .route("/", get(move || serve_embedded_index(slot)))
        .fallback(get(move || serve_embedded_index(slot)));

    let page_router = if mount_path == "/" {
        page_router
    } else {
        Router::new().nest(mount_path, page_router)
    };

    page_router.route(
        &format!("{asset_path}/{{*path}}"),
        get(move |path| serve_embedded_asset(slot, path)),
    )
}

#[cfg(feature = "frontend-embed")]
#[derive(rust_embed::RustEmbed)]
#[folder = "assets/frontend"]
struct EmbeddedFrontend;

#[cfg(feature = "frontend-embed")]
async fn serve_embedded_index(_slot: FrontendSlot) -> Response {
    embedded_asset_response("index.html").unwrap_or_else(not_found_response)
}

#[cfg(feature = "frontend-embed")]
async fn serve_embedded_asset(_slot: FrontendSlot, AxumPath(path): AxumPath<String>) -> Response {
    embedded_asset_response(&format!("assets/{path}")).unwrap_or_else(not_found_response)
}

#[cfg(feature = "frontend-embed")]
fn embedded_asset_response(path: &str) -> Option<Response> {
    let file = <EmbeddedFrontend as rust_embed::Embed>::get(path)?;
    let content_type = mime_guess::from_path(path).first_or_octet_stream();

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type.as_ref())
        .body(Body::from(file.data.into_owned()))
        .ok()
}

#[cfg(feature = "frontend-embed")]
fn not_found_response() -> Response {
    (StatusCode::NOT_FOUND, "frontend asset not found").into_response()
}
