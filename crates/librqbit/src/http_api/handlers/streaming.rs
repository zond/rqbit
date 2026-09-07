use std::{io::SeekFrom, sync::Arc};

use anyhow::Context;
use axum::{
    extract::{Path, Query, State},
    response::IntoResponse,
};
use bytes::Bytes;
use http::{HeaderMap, HeaderValue, StatusCode};
use serde::Deserialize;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt};
use tracing::{debug, trace};

use super::ApiState;
use crate::{
    FileStreamOptions, WithStatus,
    api::{Result, TorrentIdOrHash},
};

#[derive(Deserialize)]
pub struct StreamPathParams {
    id: TorrentIdOrHash,
    file_id: usize,
    #[serde(rename = "filename")]
    _filename: Option<Arc<str>>,
}

#[derive(Default, Deserialize)]
pub struct StreamQueryParams {
    lookahead_bytes: Option<u64>,
}

/// The most lookahead a request may ask for.
///
/// The lookahead is a buffering window, and it does two things to every piece it covers:
/// puts it in the picker's priority set, and keeps it from being reclaimed (see
/// [`crate::ManagedTorrent::drop_pieces`]) for as long as the stream is open. Left to the
/// file length, one request could pin a whole file with `lookahead_bytes=<huge>`. A GiB is
/// well past anything a player buffers, and a library caller with a real reason to want
/// more still has [`crate::Api::api_stream_with_options`], which this doesn't bound.
const MAX_LOOKAHEAD_BYTES: u64 = 1024 * 1024 * 1024;

fn stream_options(query: &StreamQueryParams) -> Result<FileStreamOptions> {
    match query.lookahead_bytes {
        Some(0) => Err(anyhow::anyhow!("lookahead_bytes must be positive"))
            .with_status(StatusCode::BAD_REQUEST),
        Some(lookahead_bytes) if lookahead_bytes > MAX_LOOKAHEAD_BYTES => Err(anyhow::anyhow!(
            "lookahead_bytes must be at most {MAX_LOOKAHEAD_BYTES}"
        ))
        .with_status(StatusCode::BAD_REQUEST),
        Some(lookahead_bytes) => Ok(FileStreamOptions { lookahead_bytes }),
        None => Ok(FileStreamOptions::default()),
    }
}

pub async fn h_torrent_stream_file(
    State(state): State<ApiState>,
    Path(StreamPathParams { id, file_id, .. }): Path<StreamPathParams>,
    Query(query): Query<StreamQueryParams>,
    headers: http::HeaderMap,
) -> Result<impl IntoResponse> {
    trace!(?id, ?file_id, "acquiring stream");
    let options = stream_options(&query)?;
    let mut stream = state
        .api
        .api_stream_with_options(id, file_id, options)
        .await?;
    let mut status = StatusCode::OK;
    let mut output_headers = HeaderMap::new();
    output_headers.insert("Accept-Ranges", HeaderValue::from_static("bytes"));

    const DLNA_TRANSFER_MODE: &str = "transferMode.dlna.org";
    const DLNA_GET_CONTENT_FEATURES: &str = "getcontentFeatures.dlna.org";
    const DLNA_CONTENT_FEATURES: &str = "contentFeatures.dlna.org";

    if headers
        .get(DLNA_TRANSFER_MODE)
        .map(|v| matches!(v.as_bytes(), b"Streaming" | b"streaming"))
        .unwrap_or(false)
    {
        output_headers.insert(DLNA_TRANSFER_MODE, HeaderValue::from_static("Streaming"));
    }

    if headers
        .get(DLNA_GET_CONTENT_FEATURES)
        .map(|v| v.as_bytes() == b"1")
        .unwrap_or(false)
    {
        output_headers.insert(
            DLNA_CONTENT_FEATURES,
            HeaderValue::from_static("DLNA.ORG_OP=01"),
        );
    }

    if let Ok(mime) = state.api.torrent_file_mime_type(id, file_id) {
        output_headers.insert(http::header::CONTENT_TYPE, HeaderValue::from_static(mime));
    }

    let range_header = headers.get(http::header::RANGE);
    debug!(torrent_id=%id, file_id=file_id, range=?range_header, "request for HTTP stream");

    let range = range_header
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("bytes="))
        .and_then(|v| v.split_once('-'))
        .and_then(|(start, end)| {
            let start = start.parse::<u64>().ok()?;
            let end = if end.is_empty() {
                None
            } else {
                Some(end.parse::<u64>().ok()?.saturating_add(1))
            };
            Some((start, end))
        });

    let stream: Box<dyn AsyncRead + Send + Unpin> = if let Some((start, end)) = range {
        status = StatusCode::PARTIAL_CONTENT;

        if start >= stream.len() || end.is_some_and(|end| end <= start || end > stream.len()) {
            return Err(anyhow::anyhow!("bad range"))
                .with_status(StatusCode::RANGE_NOT_SATISFIABLE);
        }

        let end = end.unwrap_or(stream.len());

        stream
            .seek(SeekFrom::Start(start))
            .await
            .context("error seeking")?;

        let to_take = end - start;

        output_headers.insert(
            http::header::CONTENT_LENGTH,
            HeaderValue::from_maybe_shared(Bytes::from(to_take.to_string())).unwrap(),
        );
        output_headers.insert(
            http::header::CONTENT_RANGE,
            HeaderValue::from_maybe_shared(Bytes::from(format!(
                "bytes {}-{}/{}",
                start,
                end.saturating_sub(1),
                stream.len()
            )))
            .unwrap(),
        );
        Box::new(stream.take(to_take))
    } else {
        output_headers.insert(
            http::header::CONTENT_LENGTH,
            HeaderValue::from_maybe_shared(Bytes::from(stream.len().to_string())).unwrap(),
        );
        Box::new(stream)
    };

    let s = tokio_util::io::ReaderStream::with_capacity(stream, 65536);
    Ok((status, (output_headers, axum::body::Body::from_stream(s))))
}

#[cfg(test)]
mod tests {
    use http::StatusCode;

    use super::{MAX_LOOKAHEAD_BYTES, StreamQueryParams, stream_options};
    use crate::DEFAULT_STREAM_LOOKAHEAD_BYTES;

    fn lookahead(lookahead_bytes: Option<u64>) -> Result<u64, StatusCode> {
        stream_options(&StreamQueryParams { lookahead_bytes })
            .map(|o| o.lookahead_bytes)
            .map_err(|e| e.status())
    }

    // The query string is untrusted input, and the lookahead it asks for is what the
    // reclaim guard and the piece picker see. Bound it here; the library API stays as
    // it is for callers that mean it.
    #[test]
    fn test_lookahead_bytes_is_bounded() {
        assert_eq!(lookahead(None), Ok(DEFAULT_STREAM_LOOKAHEAD_BYTES));
        assert_eq!(lookahead(Some(1)), Ok(1));
        assert_eq!(
            lookahead(Some(MAX_LOOKAHEAD_BYTES)),
            Ok(MAX_LOOKAHEAD_BYTES)
        );

        // Zero and too much are both refused, and refused as the client's mistake.
        assert_eq!(lookahead(Some(0)), Err(StatusCode::BAD_REQUEST));
        assert_eq!(
            lookahead(Some(MAX_LOOKAHEAD_BYTES + 1)),
            Err(StatusCode::BAD_REQUEST)
        );
        assert_eq!(lookahead(Some(u64::MAX)), Err(StatusCode::BAD_REQUEST));
    }
}
