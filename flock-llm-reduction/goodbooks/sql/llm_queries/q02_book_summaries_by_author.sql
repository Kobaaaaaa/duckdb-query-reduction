-- Groups books by author and summarizes their writing style and common themes
SELECT authors, 
       llm_reduce(
         {'model_name': 'gpt-4o'},
         {
           'prompt': 'Summarize the writing style and common themes of this author based on their book titles.', 
           'context_columns': [{'data': title}]
         }
       ) AS author_summary
FROM books
GROUP BY authors;