# Course TXT Uploader Bot

Telegram bot for extracting and uploading course videos, streams, PDFs, and images from `.txt` batch files to a Telegram Channel or Forum Group Topics.

## Features
- Password authentication, session locking, and owner premium plan management.
- Fresh token override on batch URLs and request headers.
- Topic parsing from `[Subject → Topic]`, `(...)` groups, `|` separators, and subject headers.
- Channel mode (`/channel`) and Group Forum Topic mode (`/grouptopictransfer`).
- Batch range slicing (`/range 1 50`), live download/upload progress bars, 3x auto-retry, and `/retryfailed`.
- Keep-alive HTTP health server on `PORT` for Render Web Services.
