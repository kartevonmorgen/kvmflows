<div align="center">
  <h1>Kartevonmorgen Workflows (kvmflows)</h1>
  <p><strong>Automated subscription and notification service for OpenFairDB entries</strong></p>
  <img src="https://img.shields.io/badge/python-3.12%2B-blue" alt="Python Version">
  <img src="https://img.shields.io/badge/fastapi-API-green" alt="FastAPI">
  <img src="https://img.shields.io/badge/mailgun-email-orange" alt="Mailgun">
</div>

---

## Overview

Kartevonmorgen Workflows (kvmflows) is a backend service for managing user subscriptions to OpenFairDB entries. Users can subscribe to updates or new entries in specific geographic areas and receive periodic email notifications. The system includes:

- **API Server**: Accepts subscription requests, handles activation/unsubscription, and manages user preferences.
- **Email Service**: Sends activation emails and periodic batch notifications using Mailgun and Liquid templates.
- **Cron Jobs**: Regularly scrape OpenFairDB for new/updated entries and trigger email notifications.

---

## Features

- **Subscription API**: Create, activate, and unsubscribe from area-based entry notifications.
- **Email Activation**: Users must activate subscriptions via a secure email link.
- **Batch Notifications**: Periodic emails with new/updated entries in the user's area.
- **Configurable Intervals**: Daily, weekly, and monthly notification options.
- **Robust Error Handling**: Retries, rate limiting, and logging for email delivery.

---

## Architecture

- **FastAPI**: RESTful API for subscription management.
- **Mailgun**: Email delivery for activation and batch notifications.
- **Liquid Templates**: Customizable email content.
- **PostgreSQL**: Persistent storage for subscriptions and entries.
- **APScheduler**: Cron jobs for scraping and notifications.
- **Docker Compose**: Containerized deployment for all services.

---

## Getting Started

### Prerequisites

- Python 3.12+
- Docker & Docker Compose
- [Poetry](https://python-poetry.org/) for dependency management
- Mailgun account (for email delivery)
- PostgreSQL database

### Installation

1. **Clone the repository:**
   ```sh
   git clone https://github.com/your-org/kvmflows.git
   cd kvmflows
   ```
2. **Configure environment:**
   - Edit `config.yaml` for API keys, database, and email settings.
   - Ensure Mailgun credentials and template paths are set.
3. **Install dependencies:**
   ```sh
   poetry install
   ```
4. **Run database migrations:**
   (If needed, apply migrations or run `init_pg_dbs.sql`)

### Running the Project

#### Local Development

- **Start API server:**
  ```sh
  poetry run python src/kvmflows/apis/server.py
  ```
- **Run cron jobs manually:**
  ```sh
  poetry run python src/kvmflows/crons/send_subscription_emails.py
  poetry run python src/kvmflows/crons/sync_entries.py
  ```

#### Docker Compose

- **Build and start all services:**
  ```sh
  docker-compose up --build
  ```
  This will start:
  - API server (`server.py`)
  - Entry synchronizer (`sync_entries.py`)
  - Subscription email service (`send_subscription_emails.py`)

---

## API Endpoints

- `POST /v1/subscriptions` — Create a new subscription
- `GET /v1/subscriptions/{id}/activate` — Activate a subscription
- `GET /v1/subscriptions/{id}/unsubscribe` — Unsubscribe

See [OpenAPI docs](config.yaml) for full endpoint details.

---

## Email Flow

1. **User subscribes via API.**
2. **Activation email sent** with secure link.
3. **User activates subscription.**
4. **Cron jobs** scrape OpenFairDB for new/updated entries.
5. **Batch emails** sent to users with relevant entries.
6. **User can unsubscribe at any time.**

---

## Configuration

- All settings are managed in `config.yaml`:
  - API keys, email templates, database credentials, cron schedules, area definitions, etc.
- Example email templates are in `src/kvmflows/templates/`.

---

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/foo`)
3. Commit your changes (`git commit -am 'Add feature'`)
4. Push to the branch (`git push origin feature/foo`)
5. Create a new Pull Request

---

## License

This project is licensed under the MIT License.

---

## Maintainers

- Navid Kalaei ([navidkalaei@gmail.com](mailto:navidkalaei@gmail.com))
