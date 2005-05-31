create generator catalog_item_gen;

--set generator catalog_item_gen to 1000000;

create table catalog_items (
	id numeric not null,
	uri  varchar(4000) not null,
	title varchar(4000) not null,
	primary key (id)
);

create table catalog_item_title_chars (
	catalog_item_id numeric not null,
	title_ci_char char(1) not null, /* case-cannonicalized char from the title */
	primary key (catalog_item_id, title_ci_char),
	foreign key (catalog_item_id) references catalog_items
		on delete cascade
		on update cascade
);

/* Create an index to facilitate searching for titles with a particular char
in them */
create index idx_cat_item_title_chars
on catalog_item_title_chars (title_ci_char);
