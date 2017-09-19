sources_urls_path="$1"

# descarga las fuentes de cada catálogo
while read catalog_id url; do

	# crea la carpeta de las fuentes originales del catálogo
	mkdir -p "data/input/catalog/$catalog_id/sources" ;

	# chequea que la URL está disponible y saludable
	status_code=$(curl -o /dev/null --silent --head --write-out '%{http_code}\n' $url)
	if [ $status_code == "200" ] || [ $status_code == "000" ]; then
		echo "$catalog_id $url" ;
		wget -N --directory-prefix="data/input/catalog/$catalog_id/sources" "$url" --no-check-certificate ;
	else
		echo "URL $url NO EXISTE" ;
	fi ;

done < "$sources_urls_path"
