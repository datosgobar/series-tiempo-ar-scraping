sources_urls_path="$1"

# descarga las fuentes de cada catálogo
while read catalog_id dataset_id distribution_id distribution_fileName url; do

	# crea la carpeta de las fuentes originales del catálogo
	distribution_dir="data/input/catalog/$catalog_id/dataset/$dataset_id/distribution/$distribution_id/download/"
	mkdir -p $distribution_dir ;

	# chequea que la URL está disponible y saludable
	status_code=$(curl -o /dev/null --silent --head --write-out '%{http_code}\n' $url)
	if [ $status_code == "200" ] || [ $status_code == "000" ] || [ $status_code == "302" ]; then
		echo "$catalog_id $url" ;
		wget -N -O "$distribution_dir$distribution_fileName" "$url" --no-check-certificate ;
	else
		echo "URL $url NO EXISTE" ;
	fi ;

done < "$sources_urls_path"
