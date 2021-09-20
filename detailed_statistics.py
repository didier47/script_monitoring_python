import re

import pandas

from util.common import *

REQUEST_STATE = [ERRORS_TAG,
                 INCONCLUSIVE_TAG,
                 TIMES_OUT_TAG]
SEARCH_REGEX = {
    ERRORS_TAG: r'%s.*("codError":100|Index was out of range|Sequence contains no elements|En este momento el servicio no está disponible|<!DOCTYPE HTML PUBLIC).*',
    INCONCLUSIVE_TAG: r'.*%s.*"Codigo":"01"(?!.*En este momento el servicio no está disponible).*"Mensaje":(".*")',
    TIMES_OUT_TAG: r'%s.*(SocketTimeoutException: Read timed out)'
}
EXPORT_FILE_NAME = 'logs_errores_detallados'


def export_to_dataframe(dict_total_request):
    dataframe = pandas.DataFrame.from_dict(dict_total_request, orient='index').transpose()
    dataframe = dataframe.apply(pandas.Series.value_counts)
    dataframe.to_csv(f'{EXPORT_FILE_NAME}.{CSV_EXTENSION}')
    dataframe.to_excel(f'{EXPORT_FILE_NAME}.{EXCEL_EXTENSION}')


def monitoring():
    dict_total_request = dict.fromkeys(LIST_REQUEST)
    for folder in search_file(f'{LOG_PATH_FOLDER}'):
        for log in search_file(folder):
            file_log = open(log, encoding='utf-8').read()
            for request in LIST_REQUEST:
                list_requests = re.findall(r'%s..(?:{|<--- ERROR|<!).*' % request, file_log)
                string_requests = '\n'.join(list_requests)
                if dict_total_request[request] is None:
                    dict_total_request[request] = []
                for key, value in SEARCH_REGEX.items():
                    requests_list = re.findall(value % request, string_requests)
                    dict_total_request[request] += requests_list
    export_to_dataframe(dict_total_request)


def main():
    decompress_logs(f'{LOG_PATH_FOLDER}')
    monitoring()


if __name__ == '__main__':
    main()
