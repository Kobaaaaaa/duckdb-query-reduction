-- Select books with an average rating of 4.0 or higher, keeping only the sci-fi themed ones  
SELECT book_id, title, authors, average_rating
FROM books
WHERE CAST(average_rating AS DECIMAL(2,1)) >= 4.0
  AND llm_filter(
        {'model_name': 'gpt-4o'},
        {
          'prompt': 'Is this book a science fiction novel?',
          'context_columns': [
            {'data': title}, 
            {'data': authors}
          ]
        }
      );
