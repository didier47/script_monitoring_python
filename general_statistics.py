import gzip
import os
import re
import shutil
import tarfile as tar
from glob import glob

import pandas

GZ_EXTENSION = 'gz'
TAR_EXTENSION = 'tar'
COMPRESSED_EXTENSION = [TAR_EXTENSION, GZ_EXTENSION]
LOG_EXTENSION = 'log'
LOG_PATH_FOLDER = r'.\LOGS_CMB'
ALL_FILES_CHARACTERS = '/*'
LIST_REQUEST = [
    'validarTarjetaVirtual',
    'iniciarSesion',
    'obtenerServicios',
    'obtenerCompanias',
    'obtenerPagoServicios',
    'obtenerCuentas',
    'registrarPago'
]
STATE_TAG = 'estado'
TOTAL_TAG = 'total'
APPROVED_TAG = 'aprobadas'
ERRORS_TAG = 'errores'
INCONCLUSIVE_TAG = 'inconclusas'
REQUEST_STATE = [TOTAL_TAG,
                 APPROVED_TAG,
                 ERRORS_TAG,
                 INCONCLUSIVE_TAG]
SEARCH_REGEX = {
    TOTAL_TAG: r'%s.*(?:{"Timestamp"|{"Data").*',
    APPROVED_TAG: r'%s.*(?:"codError":0|{"Codigo":"00").*',
    ERRORS_TAG: r'%s.*(?:"codError":100|Index was out of range|Sequence contains no elements|En este momento el servicio no está disponible).*',
    INCONCLUSIVE_TAG: r'.*%s.*"Codigo":"01"(?!.*En este momento el servicio no está disponible).*'
}
EXPORT_FILE_NAME = 'logs_estado_cmb'


def search_file(path):
    list_files = glob(f'{path}{ALL_FILES_CHARACTERS}')
    return list_files


def decompress_tar_file(file_path):
    compressed_file = tar.open(file_path)
    compressed_file.extractall(LOG_PATH_FOLDER)
    compressed_file.close()


def decompress_gz_file(file_path, destination_file_path):
    with gzip.open(file_path, 'rb') as file_in, open(destination_file_path, 'wb') as file_out:
        shutil.copyfileobj(file_in, file_out)


def split_file_extension(path):
    list_path_split = path.split('.')
    file_extension = list_path_split.pop(-1)
    file_path_with_no_extension = '.'.join(list_path_split)
    return file_path_with_no_extension, file_extension


def export_to_dataframe(dict_total_request):
    data_frame_logs = pandas.DataFrame(data=dict_total_request)
    data_frame_logs.insert(0, STATE_TAG, REQUEST_STATE)
    data_frame_logs.to_excel(f'{EXPORT_FILE_NAME}.xlsx', index=False)
    data_frame_logs.to_csv(f'{EXPORT_FILE_NAME}.csv', index=False)


def decompress_logs(path):
    for file in search_file(path):
        file_path_with_not_extension, file_extension = split_file_extension(file)
        if file_extension == TAR_EXTENSION and not os.path.exists(file_path_with_not_extension):
            decompress_tar_file(file)
            decompress_logs(file_path_with_not_extension)
        elif file_extension == GZ_EXTENSION:
            decompress_gz_file(file, file_path_with_not_extension)
        if os.path.exists(file_path_with_not_extension) or file_extension == GZ_EXTENSION:
            os.remove(file)


def monitoring():
    dict_total_request = dict.fromkeys(LIST_REQUEST)
    for folder in search_file(f'{LOG_PATH_FOLDER}'):
        for log in search_file(folder):
            file_log = open(log, encoding='utf-8').read()
            for request in LIST_REQUEST:
                if dict_total_request[request] is None:
                    dict_total_request[request] = {
                        TOTAL_TAG: 0,
                        APPROVED_TAG: 0,
                        ERRORS_TAG: 0,
                        INCONCLUSIVE_TAG: 0
                    }
                list_requests = re.findall(r'%s..{.*' % request, file_log)
                string_requests = '\n'.join(list_requests)
                for key, value in SEARCH_REGEX.items():
                    requests_found = len(re.findall(value % request, string_requests))
                    dict_total_request[request][key] += requests_found
    export_to_dataframe(dict_total_request)


def main():
    decompress_logs(f'{LOG_PATH_FOLDER}')
    monitoring()


if __name__ == '__main__':
    main()
