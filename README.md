# Fika Prep

Data pipeline for Fika — sets up Supabase database.

## Prerequisites

- Create a [Supabase](https://supabase.com/) project.

## Getting Started

> [!NOTE]
> Install [uv](https://docs.astral.sh/uv/getting-started/installation/) before proceeding.

### 1. Install dependencies

```bash
make
```

### 2. Set up environment

```bash
cp .env.example .env
```

### 3. Create database tables

```bash
make phase-one
```

### 4. Prepare your data

Before running phase-two, you need:

**Boundaries** — Edit `src/load_polygon.py` to configure your regions:

```python
REGIONS = [
    {"country": "Japan", "iso2": "JP", "states": ["Tokyo", "Osaka", "Kyoto"]},
]
```

**Roles** — Create `data/text/meal.txt` and `data/text/accommodation.txt` with category keywords:

```
restaurant
cafe
hotel
resort
```

**Themes** — Create `data/text/attractions/*.txt` files (e.g., `nature.txt`, `shopping.txt`):

```
national park
beach
shopping mall
```

**POIs** — Place your POI data in `output/poi.csv` with required fields:

| Field | Required | Description |
|-------|----------|-------------|
| `link` | ✅ | Unique identifier (Google Maps URL) |
| `name` | ✅ | POI name |
| `latitude` | ✅ | Coordinate |
| `longitude` | ✅ | Coordinate |
| `categories` | ✅ | Pipe or comma-separated |
| `address` | | Full address |
| `website` | | URL |
| `phone` | | Contact number |
| `timezone` | | Timezone string |
| `open_hours` | | JSON opening hours |
| `review_count` | | Integer |
| `review_rating` | | 1.0-5.0 |
| `complete_address` | | JSON address components |
| `descriptions` | | Text description |
| `price_level` | | 1.0-4.0 |
| `images` | | JSON array of URLs |
| `kids_friendly` | | Boolean |
| `pets_friendly` | | Boolean |
| `wheelchair_rental` | | Boolean |
| `wheelchair_accessible_car_park` | | Boolean |
| `wheelchair_accessible_entrance` | | Boolean |
| `wheelchair_accessible_seating` | | Boolean |
| `wheelchair_accessible_toilet` | | Boolean |
| `halal_food` | | Boolean |
| `vegan_options` | | Boolean |
| `vegetarian_options` | | Boolean |
| `reservations_required` | | Boolean |

### 5. Load data

```bash
make phase-two
```

### 6. Create database functions

```bash
make phase-three
```