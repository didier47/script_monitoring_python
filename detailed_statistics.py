import re

import pandas

from util.common import decompress_logs, LOG_PATH_FOLDER, LIST_REQUEST, search_file, INCONCLUSIVE_TAG, ERRORS_TAG

REQUEST_STATE = [ERRORS_TAG,
                 INCONCLUSIVE_TAG]
SEARCH_REGEX = {
    ERRORS_TAG: r'%s.*("codError":100|Index was out of range|Sequence contains no elements|En este momento el servicio no está disponible).*',
    INCONCLUSIVE_TAG: r'.*%s.*"Codigo":"01"(?!.*En este momento el servicio no está disponible).*"Mensaje":(".*")'
}
EXPORT_FILE_NAME = 'logs_estado_cmb'


def monitoring():
    dict_total_request = dict.fromkeys(LIST_REQUEST)
    for folder in search_file(f'{LOG_PATH_FOLDER}'):
        for log in search_file(folder):
            file_log = open(log, encoding='utf-8').read()
            for request in LIST_REQUEST:
                if dict_total_request[request] is None:
                    dict_total_request[request] = []
                list_requests = re.findall(r'%s..{.*' % request, file_log)
                string_requests = '\n'.join(list_requests)
                for key, value in SEARCH_REGEX.items():
                    requests_list = re.findall(value % request, string_requests)
                    dict_total_request[request] += requests_list
    dataframe = pandas.DataFrame.from_dict(dict_total_request, orient='index').transpose()
    dataframe = dataframe.apply(pandas.Series.value_counts)
    dataframe.to_csv('logs_detallados.csv')
    dataframe.to_excel('logs_detallados.xlsx')


def main():
    decompress_logs(f'{LOG_PATH_FOLDER}')
    monitoring()


if __name__ == '__main__':
    main()
