class donnor_analytics():
    def __init__(self):
        import os
        import numpy as np
        import datetime

    def check_valid_record(self,line):
        split_line = line.split('|')
        CMTE_ID = split_line[0]
        NAME = split_line[7]
        ZIP_CODE = split_line[10]
        TRANSACTION_DT = split_line[13]
        TRANSACTION_AMT = split_line[14]
        OTHER_ID = split_line[15]
        CMTE_ID_FLAG, NAME_FLAG, ZIP_CODE_FLAG, TRANSACTION_AMT_FLAG, TRANSACTION_DT_FLAG, OTHER_ID_FLAG = 0, 0, 0, 0, 0, 0
        # CMTE_ID
        if len(CMTE_ID) == 0:
            CMTE_ID_FLAG = 1
        # NAME
        if len(NAME) == 0:
            NAME_FLAG = 1
        # ZIP CODE
        if len(ZIP_CODE) < 5:
            ZIP_CODE_FLAG = 1
        else:
            ZIP_CODE = ZIP_CODE[:5]
        # Transaction amount
        if len(TRANSACTION_AMT) == 0:
            TRANSACTION_AMT_FLAG = 1
        else:
            TRANSACTION_AMT = float(TRANSACTION_AMT)
        # TRANSACTION date
        try:
            date_object = datetime.datetime.strptime(TRANSACTION_DT, '%m%d%Y')
            TRANSACTION_YEAR = date_object.year
        except ValueError:
            TRANSACTION_DT_FLAG = 1
        # other ID
        if len(OTHER_ID) != 0:
            OTHER_ID_FLAG = 1
        if sum([CMTE_ID_FLAG, NAME_FLAG, ZIP_CODE_FLAG, TRANSACTION_AMT_FLAG, TRANSACTION_DT_FLAG, OTHER_ID_FLAG]) == 0:
            return CMTE_ID, NAME, ZIP_CODE, TRANSACTION_YEAR, TRANSACTION_AMT, OTHER_ID  # record is valid
        else:
            return [0, 0, 0, 0, 0, 0]  # record is invalid

    def running_percentile_calculator(self,repeat_donnor_contribution_amount_list, input_percentile):
        running_percentile = int(
            round(np.percentile(repeat_donnor_contribution_amount_list, input_percentile, interpolation='nearest')))
        return running_percentile

    def write_output(self,temp):
        temp = list(map(lambda x: str(x), temp))
        temp = '|'.join(temp)
        output_file.writelines(temp + '\n')
        output_file.flush()
        os.fsync(output_file)
        return None

    def process_data(self,ledger, CMTE_ID, NAME, ZIP_CODE, TRANSACTION_YEAR, TRANSACTION_AMT, OTHER_ID, input_percentile):
        repeat_donor_flag = 0
        if ZIP_CODE not in ledger.keys():
            ledger[ZIP_CODE] = {}
            ledger[ZIP_CODE]['donor_list'] = [NAME]
            ledger[ZIP_CODE][TRANSACTION_YEAR] = {}
            ledger[ZIP_CODE][TRANSACTION_YEAR]['names'] = {}
            ledger[ZIP_CODE][TRANSACTION_YEAR]['names'][NAME] = [TRANSACTION_AMT]
        else:  # zip code already present, so need to check if the transaction year is present or not
            if NAME in ledger[ZIP_CODE]['donor_list']:  # checking if the donor has donated in the past or not
                repeat_donor_flag = 1
            else:
                ledger[ZIP_CODE]['donor_list'].append(NAME)

            if TRANSACTION_YEAR not in ledger[ZIP_CODE].keys():  # adding the donors' record to current year
                ledger[ZIP_CODE][TRANSACTION_YEAR] = {}
                ledger[ZIP_CODE][TRANSACTION_YEAR]['names'] = {}
                ledger[ZIP_CODE][TRANSACTION_YEAR]['names'][NAME] = [TRANSACTION_AMT]
                ledger[ZIP_CODE][TRANSACTION_YEAR]['repeat_donnor_count'] = 0
                ledger[ZIP_CODE][TRANSACTION_YEAR]['repeat_donnor_contribution'] = 0
                ledger[ZIP_CODE][TRANSACTION_YEAR]['repeat_donnor_contribution_amount_list'] = []
            else:  # transaction year already present, check if name present or not
                if NAME not in ledger[ZIP_CODE][TRANSACTION_YEAR]['names'].keys():
                    ledger[ZIP_CODE][TRANSACTION_YEAR]['names'][NAME] = [TRANSACTION_AMT]
                else:  # name already present, we have a repeat donnor for the year
                    ledger[ZIP_CODE][TRANSACTION_YEAR]['names'][NAME].append(TRANSACTION_AMT)
            if repeat_donor_flag == 1:
                ledger[ZIP_CODE][TRANSACTION_YEAR]['repeat_donnor_count'] += 1
                ledger[ZIP_CODE][TRANSACTION_YEAR]['repeat_donnor_contribution_amount_list'].append(TRANSACTION_AMT)
                ledger[ZIP_CODE][TRANSACTION_YEAR]['repeat_donnor_contribution'] += int(
                    round(sum(ledger[ZIP_CODE][TRANSACTION_YEAR]['names'][NAME])))
                repeat_donnor_contribution_amount_list = ledger[ZIP_CODE][TRANSACTION_YEAR][
                    'repeat_donnor_contribution_amount_list']
                running_percentile = self.running_percentile_calculator(repeat_donnor_contribution_amount_list,
                                                                   input_percentile)
                temp = [CMTE_ID, ZIP_CODE, TRANSACTION_YEAR, running_percentile,
                        ledger[ZIP_CODE][TRANSACTION_YEAR]['repeat_donnor_contribution'],
                        ledger[ZIP_CODE][TRANSACTION_YEAR]['repeat_donnor_count']]
                self.write_output(temp)
        return None

    def read_input_directory(self,input_path, stream):
        input_itcont = open(input_path)
        itcont_contents = []
        ledger = dict()
        while True:
            line_in_itcont = input_itcont.readline()
            if len(line_in_itcont) > 3:  # length is taken at random, to check if the line is not just '\n' charater
                CMTE_ID, NAME, ZIP_CODE, TRANSACTION_YEAR, TRANSACTION_AMT, OTHER_ID = self.check_valid_record(
                    line_in_itcont)
                if type(NAME) != int:  # if the record if invalid the above function will return all 0's an int type
                    self.process_data(ledger, CMTE_ID, NAME, ZIP_CODE, TRANSACTION_YEAR, TRANSACTION_AMT, OTHER_ID,
                                 input_percentile)
                else:
                    pass
                    # record not valid
            if stream == False and line_in_itcont == '':
                break
                # we reached end of file, loop terminated as we are not working with stream of data.
            else:
                pass
                # we are working with stream of data, thus the loop does not terminate
        return None


if __name__ == '__main__':
    import os
    import numpy as np
    import datetime
    import sys
    import string
    obj = donnor_analytics()
    current_path = os.getcwd()
    global output_file
    #input_path = current_path + '\\input\\itcont.txt'
    #output_path = current_path + '\\output\\repeat_donors.txt'
    #percentile_path = current_path + '\\input\\percentile.txt'
    input_path = sys.argv[1]
    percentile_path = sys.argv[2]
    output_path = sys.argv[3]

    global input_percentile
    input_percentile = open(percentile_path, 'r')
    input_percentile = float(input_percentile.readline()[:-1])
    output_file = open(output_path, 'w+')

    # make  stream  below True if working with stream of data and program will not terminate; make it False if not working with stream of data,
    # which means the program will terminate when we reach end of itcont.txt file.
    obj.read_input_directory(input_path, stream=False)
