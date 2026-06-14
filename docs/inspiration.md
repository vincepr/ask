# Inspiration Repos/Articles/Blogposts

# qmd
Looks really cool. Sadly javascript: https://github.com/tobi/qmd
* uses combined https://sqlite.org/fts5.html with vectorsearch (uses small (trained?) model to get search-prompt splits with double weighted og-prompt)

## Thoughts
- Should do proper ingest with -> cunking-pipeline -> parser (so we can be flexible while adding tokens?) -> token/context based chunking split in stratgies (simple-byte-chunks / regex based borders/ tree-sitter with added namespace/class context added optional later for .cs, .js files?)
- It uses small pretrained model to fan out simplie query into more context (cool concept and can always be added later without much problems)
- fts5 for raw text based keyword search. Should definitly add this (either as other mode or weighted mode). Is it worth to abstract here? IQuery: (context, RatedDocumentResults[]) => RatedDocumentResults[] : Fts5_Query | Embedding_Query | DoBothInParallelAndCombineIfNotTimeout_Query
- Stores all text in db and embeddings just link to it. We should not do that. BUT i think we should just hash the whole/embedded document -> easy way to look for changes, till we do more complex file watcher stuff (if at all).
- After results are done it also uses llm to rerank the results. Can also be added as optional later step. By just adding a new IQuery. As long as IQuery can get previous outputs as input.

# Collection
Collection of memory related projects: 
https://github.com/aristoapp/awesome-second-brain

