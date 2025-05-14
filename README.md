# Tracking Stress Tool

A stress testing tool for tracking systems that includes impressions, clicks, and conversions. This tool is designed to generate high volumes of tracking events to test the capacity and performance of analytics or advertising tracking systems.

## Features

- Generates realistic tracking events (impressions, clicks, conversions) with detailed mock data
- Multi-threaded operation to simulate multiple concurrent users
- Configurable event generation rates, timeouts, and probabilities
- PostgreSQL database integration for storing all events and responses
- Flexible configuration via command-line arguments
- Docker support for easy database setup
- Detailed reporting and metrics

## Requirements

- Python 3.13+
- Docker and Docker Compose (for PostgreSQL database)

### Python Dependencies

- Faker (for generating realistic user data)
- psycopg2-binary (for PostgreSQL connectivity, optional if not using database)
- requests (for HTTP requests)

Install dependencies:

```bash
pip install faker psycopg2-binary requests
```

## Getting Started

### 1. Setup PostgreSQL Database (Optional)

The tool can store all events in a PostgreSQL database. To set up the database using Docker:

```bash
# Start the PostgreSQL container
docker-compose up -d
```

This will start a PostgreSQL instance with:
- Database name: tracking_events
- Username: trackuser
- Password: trackpass
- Port: 5432

### 2. Run the Stress Test

#### Basic Usage (No Database)

```bash
# Run a basic test with 10 impressions
python main.py

# Run a multi-threaded test with 5 concurrent users for 2 minutes
python main.py --threads 5 --duration 120

# Run in dry-run mode (no actual HTTP requests)
python main.py --dry-run
```

#### With Database Storage

```bash
# Run with database storage enabled
python main.py --use-database

# Run multi-threaded test with database storage
python main.py --threads 8 --duration 300 --use-database
```

## Command-Line Arguments

### General Options

- `--impressions`: Number of impressions to generate in single-threaded mode (default: 10)
- `--delay`: Delay between impression generations in seconds (default: 1.0)
- `--request-timeout`: Timeout for HTTP requests in milliseconds (default: 500)
- `--dry-run`: Do not post events, just print them
- `--hosts`: Comma-separated list of hosts to use instead of defaults
- `--seed`: Random seed for reproducible data generation
- `--threads`: Number of concurrent user threads to simulate
- `--duration`: Duration of the stress test in seconds when using threads (default: 60)

### Database Options

- `--use-database`: Store events in PostgreSQL database
- `--db-host`: PostgreSQL hostname (default: localhost)
- `--db-port`: PostgreSQL port (default: 5432)
- `--db-name`: PostgreSQL database name (default: tracking_events)
- `--db-user`: PostgreSQL username (default: trackuser)
- `--db-password`: PostgreSQL password (default: trackpass)

## Database Schema

The PostgreSQL database uses a simple schema to store events:

```sql
CREATE TABLE events (
  id SERIAL PRIMARY KEY,
  event_id VARCHAR(255) NOT NULL,
  event_type VARCHAR(20) NOT NULL,  -- 'impression', 'click', or 'conversion'
  created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  full_event JSONB NOT NULL,  -- Complete event data
  host_responses JSONB NOT NULL  -- Responses from all hosts
);
```

## Event Generation

The tool generates three types of events:

1. **Impressions**: Generated at the configured rate, each with 4-10 random ads
2. **Clicks**: Generated with 40% probability after each impression
3. **Conversions**: Generated with 30% probability after each click

Each event follows the detailed JSON format as specified in the requirements.

## Example Workflows

### Load Testing a Tracking System

```bash
# Start the database
docker-compose up -d

# Run a high-volume test with 10 concurrent users for 5 minutes
python main.py --threads 10 --duration 300 --delay 0.1 --use-database
```

### Generating Sample Data for Analysis

```bash
# Generate 1000 impressions in dry-run mode and store in database
python main.py --impressions 1000 --dry-run --use-database
```

### Testing with Specific Host Endpoints

```bash
# Test against custom endpoints
python main.py --hosts "http://tracking.example.com,http://backup-tracking.example.com" --threads 3
```

## Contributing

Contributions are welcome! Please feel free to submit pull requests or create issues for bugs and feature requests.

## License

This project is licensed under the MIT License - see the LICENSE file for details.