#!/bin/bash

xls_path="${XL_IDP_PATH_REFERENCE_INN_BY_API}/"


csv_path="${xls_path}"/csv
if [ ! -d "$csv_path" ]; then
  mkdir "${csv_path}"
fi

done_path="${xls_path}"/done
if [ ! -d "$done_path" ]; then
  mkdir "${done_path}"
fi

json_path="${xls_path}"/json
if [ ! -d "$json_path" ]; then
  mkdir "${json_path}"
fi

find "${xls_path}" -maxdepth 1 -type f \( -name "*.xls*" -or -name "*.XLS*" \) ! -newermt '3 seconds ago' -print0 | while read -d $'\0' file
do
  if [[ "${file}" == *"error_"* ]];
  then
    continue
  fi

	mime_type=$(file -b --mime-type "$file")
  echo "'${file} - ${mime_type}'"

  csv_name="${csv_path}/$(basename "${file}").csv"

  if [[ ${mime_type} = "application/vnd.ms-excel" ]]
  then
    echo "Will convert XLS '${file}' to CSV '${csv_name}'"
    in2csv -f xls "${file}" > "${csv_name}"
  elif [[ ${mime_type} = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" ]]
  then
    echo "Will convert XLSX or XLSM '${file}' to CSV '${csv_name}'"
    in2csv -f xlsx "${file}" > "${csv_name}"
  else
    echo "ERROR: unsupported format ${mime_type}"
    mv "${file}" "${xls_path}/error_$(basename "${file}")"
    continue
  fi

  if [ $? -eq 0 ]
	then
	  mv "${file}" "${done_path}"
	else
	  mv "${file}" "${xls_path}/error_$(basename "${file}")"
	  echo "ERROR during convertion ${file} to csv!"
	  continue
	fi

	# Will convert csv to json
	exit_message=$(python3 ${XL_IDP_PATH_REFERENCE_INN_BY_API_SCRIPTS}/scripts/main.py "${csv_name}" "${json_path}" 2>&1 > /dev/null)

  exit_code=$?
  echo "Exit code ${exit_code}"
  if [[ ${exit_code} == 0 ]]
	then
	  mv "${csv_name}" "${done_path}"
	else
    mv "${csv_name}" "${xls_path}/error_${exit_message}_$(basename "${csv_name}")"
	fi

done