# goodbooks

## Dataset

Goodbooks-10k contains ratings for 10,000 popular books from Goodreads: about 1 million ratings across 53,424 users.

## Data

- Full: `data/original_data/`
- Sample: `data/samples/`

## Key files:

- `ratings.csv` - user ratings (1 to 5 stars): `book_id, user_id, rating`
- `books.csv` - metadata: title, author, year, average rating, etc.
- `book_tags.csv` - user-assigned shelves/genres with counts (e.g. "fantasy", "sci-fi")
- `tags.csv` - tag ID to name mapping
- `to_read.csv` - books marked "to read" per user

## Common joins

- `ratings.book_id = books.book_id`
- `book_tags.book_id = books.book_id`
- `book_tags.tag_id = tags.tag_id`
- `to_read.book_id = books.book_id`

Load tables with: `sql/setup/load.sql`.