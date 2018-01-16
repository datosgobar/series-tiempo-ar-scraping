catalogs_urls_path="$1"
catalogs_local_dir="$2"

# descarga cada catalogo en una carpeta propia
while read catalog_id catalog_format url; do

	# crea la carpeta del catálogo a descargar
	mkdir -p "$catalogs_local_dir/$catalog_id/" ;

	# chequea si el catálogo está en Excel
	if [ $catalog_format == "xlsx" ]; then

		# chequea que la URL está disponible y saludable
		status_code=$(curl -o /dev/null --silent --head --write-out '%{http_code}\n' $url)
		if [ $status_code == "200" ] || [ $status_code == "000" ] || [ $status_code == "302" ]; then
			echo "$catalog_id $catalog_format $url" ;
			wget -N -O --tries=3 "$catalogs_local_dir/$catalog_id/catalog.xlsx" "$url" --no-check-certificate ;
		else
			echo "URL $url NO EXISTE" ;
		fi ;

	# chequea si el catálogo está en data.json
	elif [ $catalog_format == "json" ]; then

		# chequea que la URL está disponible y saludable
		status_code=$(curl -o /dev/null --silent --head --write-out '%{http_code}\n' $url)
		if [ $status_code == "200" ] || [ $status_code == "000" ] || [ $status_code == "302" ]; then
			echo "$catalog_id $catalog_format $url" ;
			wget -N -O --tries=3 "$catalogs_local_dir/$catalog_id/data.json" "$url" --no-check-certificate ;
		else
			echo "URL $url NO EXISTE" ;
		fi ;

	else
		echo "El formato $catalog_format de $catalog_id no se puede procesar." ;
	fi ;

done < "$catalogs_urls_path"
