import re

import pandas

from util.common import *

REQUEST_STATE = [TOTAL_TAG,
                 APPROVED_TAG,
                 ERRORS_TAG,
                 INCONCLUSIVE_TAG,
                 NO_RESPONSE_TAG]
SEARCH_REGEX = {
    TOTAL_TAG: r'%s.*(?:{"Timestamp"|{"Data").*',
    APPROVED_TAG: r'%s.*(?:"codError":0|{"Codigo":"00").*',
    ERRORS_TAG: r'%s.*(?:"codError":100|Index was out of range|Sequence contains no elements|En este momento el servicio no está disponible).*',
    INCONCLUSIVE_TAG: r'.*%s.*"Codigo":"01"(?!.*En este momento el servicio no está disponible).*',
    NO_RESPONSE_TAG: r'%s.*(SocketTimeoutException: Read timed out)'
}
EXPORT_FILE_NAME = 'logs_estado_peticiones'


def export_to_dataframe(dict_total_request):
    data_frame_logs = pandas.DataFrame(data=dict_total_request)
    data_frame_logs.insert(0, STATE_TAG, REQUEST_STATE)
    data_frame_logs.to_csv(f'{EXPORT_FILE_NAME}.{CSV_EXTENSION}', index=False)
    data_frame_logs.to_excel(f'{EXPORT_FILE_NAME}.{EXCEL_EXTENSION}', index=False)


def monitoring():
    dict_total_request = dict.fromkeys(LIST_REQUEST)
    for folder in search_file(f'{LOG_PATH_FOLDER}'):
        for log in search_file(folder):
            try:
                file_log = open(log, encoding='utf-8').read()
            except:
                print(f'Error descomprimiendo carpeta: {folder}')
            for request in LIST_REQUEST:
                list_requests = re.findall(r'%s..(?:{|<--- ERROR).*' % request, file_log)
                string_requests = '\n'.join(list_requests)
                if dict_total_request[request] is None:
                    dict_total_request[request] = {
                        TOTAL_TAG: 0,
                        APPROVED_TAG: 0,
                        ERRORS_TAG: 0,
                        INCONCLUSIVE_TAG: 0,
                        NO_RESPONSE_TAG: 0
                    }
                for key, value in SEARCH_REGEX.items():
                    requests_found = len(re.findall(value % request, string_requests))
                    dict_total_request[request][key] += requests_found
    export_to_dataframe(dict_total_request)


def main():
    decompress_logs(f'{LOG_PATH_FOLDER}')
    monitoring()


if __name__ == '__main__':
    main()
