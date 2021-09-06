import gzip
import os
import shutil
import tarfile as tar
from glob import glob

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
