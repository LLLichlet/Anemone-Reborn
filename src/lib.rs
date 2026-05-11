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

pub mod bridge;
pub mod config;
pub mod discord_client;
pub mod discord_sender;
pub mod error;
pub mod matrix_client;
pub mod matrix_sender;
pub mod message;
pub mod onebot_api;
pub mod onebot_types;
pub mod proxy;
pub mod qq_client;
pub mod qq_sender;
pub mod sender;
pub mod store;
pub mod telegram_client;
pub mod telegram_sender;

pub mod bot_controller;
pub mod log_buffer;
