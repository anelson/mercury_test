/* Lamely, must use a different terminator since ';' is used within the proc */
set term go;

create procedure sp_SearchCatalog(
	search_term varchar(200)
) returns (catalog_item_id numeric, title varchar(4000), uri varchar(4000)) as
begin


end
go

create procedure sp_AddCatalogItem (
	v_uri varchar(4000),
	v_title varchar(4000)
) returns (v_catalog_item_id numeric)
as

begin

	v_catalog_item_id = gen_id(catalog_item_gen, 1);

	insert into catalog_items(id, uri, title)
	values (:v_catalog_item_id, :v_uri, :v_title);

	-- Return the catalog item ID of the new item
	suspend;
end
go

create procedure sp_AddCatalogItemTitleChar (
	v_catalog_item_id numeric,
	v_title_char char(1)
)
as

begin
	if (not exists(select catalog_item_id from catalog_item_title_chars where catalog_item_id = :v_catalog_item_id and title_ci_char = :v_title_char)) then
	begin
		insert into catalog_item_title_chars(catalog_item_id, title_ci_char)
		values (:v_catalog_item_id, :v_title_char);
	end	
end
go

create procedure sp_DeleteCatalogItem (
	v_catalog_item_id numeric
)
as
begin
	delete from catalog_items where id = :v_catalog_item_id;
end
go

set term ; go
