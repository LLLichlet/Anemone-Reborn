/*
    Anemone-bot is a message forwarding bot that connects various chat platforms.
    Copyright (C) 2026  LLLichlet

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

use crate::error::AnemoneBotError;

/// Build a `reqwest::Client`, optionally routing through an HTTP proxy.
pub fn build_reqwest_client(proxy_url: Option<&str>) -> Result<reqwest::Client, AnemoneBotError> {
    let mut builder = reqwest::Client::builder();
    if let Some(proxy) = proxy_url {
        builder = builder.proxy(reqwest::Proxy::all(proxy)?);
    }
    Ok(builder.build()?)
}

/// Download raw bytes from a URL. Used by senders to fetch image data before re-uploading.
pub async fn download_bytes(
    url: &str,
    client: &reqwest::Client,
) -> Result<Vec<u8>, AnemoneBotError> {
    let resp = client.get(url).send().await?;
    Ok(resp.bytes().await?.to_vec())
}
