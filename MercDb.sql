PRAGMA auto_vacuum = 1;

create table catalog_items (
	catalog_item_id integer primary key autoincrement,
	uri  text not null,
	title text not null
);
create index idx_catalog_items_id
on catalog_items(catalog_item_id);

/* Create a table to store the words in each catalog title
To support multiple word tokenization algorithms, a single catalog
item title can have multiple words at the same ordinal position
to reflect multiple possible interpretations of the words in the title */
create table catalog_item_title_words (
	catalog_item_id integer not null,
	ordinal integer not null,
	word_id integer not null
);

/* table to hold all words that appear in titles */
create table title_words (
	word_id integer primary key autoincrement,
	word text not null,
	one_chars text not null, /* the first character of the word */
	two_chars text null, /* the first two characters of the word */
	three_chars text null, /* the first three characters of the word */
	four_chars text null, /* the first four characters of the word */
	five_chars text null /* the first five characters of the word */
);

/* create indices on each of the five prefix columns, and the word itself */
create index idx_title_words_word on title_words(word);
create index idx_title_words_one_chars on title_words(one_chars);
create index idx_title_words_two_chars on title_words(two_chars);
create index idx_title_words_three_chars on title_words(three_chars);
create index idx_title_words_four_chars on title_words(four_chars);
create index idx_title_words_five_chars on title_words(five_chars);

/* create a table to hold the word graph, which stores the graph of words
in the catalog file names */
create table title_word_graph (
	node_id integer primary key autoincrement,
	prev_node_id integer null,
	word_id integer not null,
	ordinal integer not null /* how many nodes down from the root is this node */
);

/* create an index for looking up words regardless of
preceeding word */
create index idx_title_word_graph_word
on title_word_graph(word_id);

/* create an index for looking up all words that follow a given node */
create index idx_title_word_graph_pwi
on title_word_graph(prev_node_id);

/* create an index for checking if a given node follows another given word */
create index idx_title_word_graph_pwiwi
on title_word_graph(prev_node_id, word_id);

/* create an index for getting a node id given the word, prev node, and ordinal */
create index idx_title_word_graph_wpwo
on title_word_graph(word_id, prev_node_id, ordinal);

/* table to hold the list of descendant nodes for each node in the graph */
create table title_word_graph_node_descendants (
	node_id integer not null,
	descendant_node_id integer not null,
	primary key (node_id, descendant_node_id)
);

/* an index to get all descendants for a given node */
create index idx_title_word_graph_node_descendants_nid
on title_word_graph_node_descendants(node_id);

/* create a table to track which items' titles are included in the subtree formed by each
title word graph node and all child nodes. */
create table title_word_graph_node_items (
	node_id integer not null,
	catalog_item_id integer not null,
	primary key (node_id, catalog_item_id)
);

/* create an index for getting the catalog items included under a node */
create index idx_title_word_graph_node_items_node
on title_word_graph_node_items(node_id);
