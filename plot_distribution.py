import matplotlib.pyplot as plt
import pandas as pd
from pathlib import Path
import seaborn as sns
import scipy
import numpy as np
from typing import Tuple, List


from matplotlib import font_manager, rc
font_path = "C:/Windows/Fonts/NGULIM.TTF"
font = font_manager.FontProperties(fname=font_path).get_name()
rc('font', family=font)


def ecdf(data):
    ''' 경험적 누적 확률 분포 '''
    n = len(data)
    x = np.sort(data)

    y = np.arange(1, n+1)/n
    return x, y

def calc_number_of_workplaces_and_energy_within_range_using_ecdf(
    data:pd.Series, energy_range:Tuple[int])->Tuple[float]:

    ''' 경험적 누적확률분포를 이용하여 구간 내 사업장의 비율, 에너지를 반환 '''

    if len(energy_range)!=2:
        raise ValueError('length of range should be 2')

    # 경험적 누적확률분포 생성
    x, y = ecdf(data)
    start, end = energy_range[0], energy_range[1] 
    start_index = np.abs(x-start).argmin()
    end_index = np.abs(x-end).argmin()
    # print(f'start index : {start_index}, end_index : {end_index}')

    # 처음 데이터는 비율이 제외되지 않도록 처리
    if start != 0:
        proportion = y[end_index]-y[start_index]
    else: 
        proportion = y[end_index]

    # print(f'start : {y[start_index]}, end : {y[end_index]}, proportion : {proportion}')
    
    # 마지막 데이터의 에너지가 합산되도록 처리
    if end_index == (len(x)-1):
        energy = sum(x[start_index:])
    else:
        energy = sum(x[start_index:end_index])

    return proportion, energy



def calc_number_of_workplaces_and_p_f_energy_within_range_using_ecdf(
    energy_dataframe:pd.DataFrame, 
    primary_energy_col:str, 
    final_energy_col:str, 
    energy_range:Tuple[int])->Tuple[float]:

    ''' 경험적 누적확률분포를 이용하여 구간 내 사업장의 비율, 평균 1차, 최종에너지를 반환 '''

    # energy_dataframe = energy
    # primary_energy_col = primary_energy_col
    # final_energy_col = final_energy_col
    # energy_range = (0, 500)


    if len(energy_range)!=2:
        raise ValueError('length of range should be 2')

    # 경험적 누적확률분포 생성
    energy_dataframe = energy_dataframe.sort_values(by=primary_energy_col)
    data = energy_dataframe[primary_energy_col].copy()

    x, y = ecdf(data)
    start, end = energy_range[0], energy_range[1] 
    # start_index = np.abs(x-start).argmin()
    # end_index = np.abs(x-end).argmin()

    # start, end를 이상인 처음 index를 찾음
    start_index = np.argmax(x>=start)
    end_index = np.argmax(x>=end)

    # 종료값이 0이 아닌데 end_index가 0인 경우, end_index를 가장 마지막 index로 지정
    if (end!=0) and (end_index == 0):
        end_index = np.argmax(x)

    # print(f'start index : {start_index}, end_index : {end_index}')

    # 누적확률분포는 해당 데이터를 포함하여 해당 데이터 이하의 값의 비율을 의미함에 유의
    # 처음 데이터는 비율이 제외되지 않도록 처리
    # 시작이 처음이고, 종료가 데이터의 마지막이 아닌 경우
    if (start == 0) and (end_index != (len(x)-1)):
        proportion = y[end_index-1]
        length = len(y[:end_index])
        primary_energy = sum(x[:end_index])
        final_energy = sum(energy_dataframe.iloc[:end_index][final_energy_col])

    # 시작이 처음이 아니고, 종료가 데이터 마지막인 경우 : 마지막 데이터가 합산되도록 처리
    elif (start != 0) and (end_index == (len(x)-1)):
        proportion = y[end_index]-y[start_index-1]
        length = len(x[start_index:])
        primary_energy = sum(x[start_index:])
        final_energy = sum(energy_dataframe.iloc[start_index:][final_energy_col])

    # 데이터 중간 영역에서 처리
    else:
        proportion = y[end_index-1]-y[start_index-1]
        length = len(y[start_index:end_index])
        primary_energy = sum(x[start_index:end_index])
        final_energy = sum(energy_dataframe.iloc[start_index:end_index][final_energy_col])



    # if start != 0 and end_index != (len(x)-1):

    # else: 
    #     proportion = y[end_index]-y[start_index]
    #     length = len(y[start_index+1:end_index+1])

    # # print(f'start : {y[start_index]}, end : {y[end_index]}, proportion : {proportion}')
    
    # # 마지막 데이터의 에너지가 합산되도록 처리
    # if end_index == (len(x)-1):
    #     primary_energy = sum(x[start_index+1:])
    #     final_energy = sum(energy_dataframe.iloc[start_index+1:][final_energy_col])
    #     length = len(x[start_index+1:])
    # else:
    #     # start_index의 값은 제외, end_index의 값은 포함
    #     primary_energy = sum(x[start_index+1:end_index+1])
    #     final_energy = sum(energy_dataframe.iloc[start_index+1:end_index+1][final_energy_col])

    return proportion, primary_energy, final_energy, length


def get_list_of_number_of_workplaces_and_energy_by_range(
    energy_data:pd.Series, energy_ranges:List[Tuple], number_of_workplaces)->Tuple[List]:
    ''' 에너지데이터에 대해 에너지구간별로 사업장의 개수와 에너지사용량을 반환 '''

    number_of_workplaces_calculated = []
    sum_of_energys_calculated = []

    for energy_range in energy_ranges:
        # energy_range = energy_ranges[0]
        proportion, energy = \
            calc_number_of_workplaces_and_energy_within_range_using_ecdf(energy_data, energy_range)
        # number_of_workplaces_calculated.append(proportion*number_of_workplaces)
        number_of_workplaces_calculated.append(round(proportion*number_of_workplaces))
        sum_of_energys_calculated.append(energy)

    return number_of_workplaces_calculated, sum_of_energys_calculated

def get_list_of_number_of_workplaces_and_p_f_energy_by_range(
    energy_dataframe:pd.DataFrame, energy_ranges:List[Tuple], number_of_workplaces)->Tuple[List]:
    ''' 에너지데이터에 대해 에너지구간별로 사업장의 개수와 1차, 최종 에너지사용량을 반환 '''

    number_of_workplaces_calculated = []
    sum_of_primary_energys_calculated = []
    sum_of_final_energys_calculated = []

    for energy_range in energy_ranges:
        # energy_range = energy_ranges[0]
        proportion, primary_energy, final_energy, length = \
            calc_number_of_workplaces_and_p_f_energy_within_range_using_ecdf(
                energy_dataframe, primary_energy_col, final_energy_col, energy_range)
        # number_of_workplaces_calculated.append(proportion*number_of_workplaces)
        number_of_workplaces_calculated.append(round(proportion*number_of_workplaces))
        sum_of_primary_energys_calculated.append(primary_energy/length*round(proportion*number_of_workplaces))
        sum_of_final_energys_calculated.append(final_energy/length*proportion*number_of_workplaces)

    return number_of_workplaces_calculated, sum_of_primary_energys_calculated, sum_of_final_energys_calculated


if __name__ == '__main__':

    filepath = Path(r'D:\0. 통계분석실(2022)\1. 요구자료\220215 산업부 신고기준 강화')
    filepath = Path(r'D:\업무\220217 대상확대')
    filename = '열량(전력최종)_업체현황_20220215.xlsx'
    filename = '2019년 업체 에너지소비 구간별.xlsx'
    sheet_name = '2019'

    # data = pd.read_excel(filepath/filename, sheet_name=sheet_name)
    final_energy_col = '열량(toe,전력최종기준)_수송제외'
    primary_energy_col = '열량(toe,전력1차기준)_수송제외'
    worker_no_col = '종사자수'

    # energy = data[[final_energy_col,
    #                primary_energy_col, worker_no_col]]
    # energy.to_pickle('energy.pkl')
    energy = pd.read_pickle('energy.pkl')

    # 모든 항목이 null 이 아닌 데이터만 처리
    energy = energy[energy.notnull().all(axis=1)]

    # 구간화
    ranges1 = [0, 500, 1000, 1500, 2000, np.inf]

    energy['bin'] = pd.cut(
        energy[primary_energy_col], ranges1, right=False)

    pt = pd.pivot_table(
        energy, index='bin',
        values=[final_energy_col, primary_energy_col],
        aggfunc='sum'
    )

    
    # 설계 기준
    # 종사자 10인 미만 사업장 개수 : 전체 산업부문 사업장 개수 - 10인 이상 사업장 개수
    # 매년 변경될 수 있으며, 아래 수치는 2019년 실적 기준
    total_number_of_workplaces = 438940
    total_number_of_workplaces_sampled = 33572
    total_number_of_workplaces_total_inspection = 69253
    # 모집단은 10인 미만으로 한정 (10인 이상 사업장은 전수조사로 검토 불필요)
    total_number_of_workplaces_population = \
        total_number_of_workplaces - total_number_of_workplaces_total_inspection

    total_number_of_workplaces_intended = \
        total_number_of_workplaces_sampled + total_number_of_workplaces_total_inspection
    print(f'[설계 기준] 데이터 총 개수 : {total_number_of_workplaces_intended:,.0f} | 10인 미만 사업장 개수 : {total_number_of_workplaces_sampled:,.0f}')        

    # 조사 결과 기준
    # 종사자 10인 미만 표본 조사 결과
    is_below_10_person = energy[worker_no_col] < 10
    energy_b10p = energy.loc[is_below_10_person].copy()
    energy_total_inspected = energy.loc[~is_below_10_person].copy()

    final_energy_b10p = energy.loc[is_below_10_person, final_energy_col]
    primary_energy_b10p = energy.loc[is_below_10_person, primary_energy_col]
    number_of_workplaces_inpected = len(energy)
    number_of_workplaces_inpected_below_10_person = len(final_energy_b10p)
    print(f'[조사 결과 기준] 데이터 총 개수 : {number_of_workplaces_inpected:,.0f} | 10인 미만 사업장 개수 : {number_of_workplaces_inpected_below_10_person:,.0f}')

    # energy_data = primary_energy_b10p
    # number_of_workplaces_primary_sampled, sum_of_energy_primary_sampled = \
    #     get_list_of_number_of_workplaces_and_energy_by_range(
    #         energy_data = energy_data,
    #         energy_ranges = [(0, 500), (500, 1000), (1000, 1500), (1500, 2000), (2000, max(energy_data))],
    #         number_of_workplaces = number_of_workplaces_inpected_below_10_person)

    max_primary_energy = energy[primary_energy_col].max()
    number_of_workplaces_primary_sampled, sum_of_primary_energy_sampled, sum_of_final_energy_sampled  = \
        get_list_of_number_of_workplaces_and_p_f_energy_by_range(
            energy_dataframe = energy_b10p,
            energy_ranges = [(0, 500), (500, 1000), (1000, 1500), (1500, 2000), (2000, max_primary_energy)],
            number_of_workplaces = number_of_workplaces_inpected_below_10_person)

    print(number_of_workplaces_primary_sampled, sum_of_primary_energy_sampled, sum_of_final_energy_sampled)

    number_of_workplaces_primary_extended, sum_of_primary_energy_extended, sum_of_final_energy_extended  = \
        get_list_of_number_of_workplaces_and_p_f_energy_by_range(
            energy_dataframe = energy_b10p,
            energy_ranges = [(0, 500), (500, 1000), (1000, 1500), (1500, 2000), (2000, max_primary_energy)],
            number_of_workplaces = total_number_of_workplaces_population)

    print(number_of_workplaces_primary_extended, sum_of_primary_energy_extended, sum_of_final_energy_extended)

    max_primary_energy = energy_total_inspected[primary_energy_col].max()
    number_of_workplaces_total_inspected, sum_of_primary_energy_total_inspected, sum_of_final_energy_total_inspected  = \
        get_list_of_number_of_workplaces_and_p_f_energy_by_range(
            energy_dataframe = energy_total_inspected,
            energy_ranges = [(0, 500), (500, 1000), (1000, 1500), (1500, 2000), (2000, max_primary_energy)],
            number_of_workplaces = len(energy_total_inspected))

    print(number_of_workplaces_total_inspected, sum_of_primary_energy_total_inspected, sum_of_final_energy_total_inspected)

    result = pd.DataFrame(
        index = pt.index,
        data = {
            '표본조사 사업장 개수' : number_of_workplaces_primary_sampled,
            '표본조사 1차에너지' : sum_of_primary_energy_sampled, 
            '표본조사 최종에너지' : sum_of_final_energy_sampled,

            '전수조사 사업장 개수' : number_of_workplaces_total_inspected, 
            '전수조사 1차에너지' : sum_of_primary_energy_total_inspected, 
            '전수조사 최종에너지' : sum_of_final_energy_total_inspected,

        }
    )

    result.to_excel('result.xlsx')

    