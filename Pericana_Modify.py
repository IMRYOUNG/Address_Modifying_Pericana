import pandas as pd

filename = "D:/Crawling_data/user/pericana.csv"
pericana_table = pd.read_csv(filename, encoding='CP949', index_col=0, header=0, engine='python')
#print(pericana_table.head()
#print(pericana_table.sido.unique() )


#1)필요없는 값  ‘0’ 제거 : drop( ) // 즉 0의 값이 어디 있는지 찾아야한다.
#print(pericana_table[pericana_table['sido']=='00'])
pericana_table = pericana_table.drop(pericana_table.index[1127])
#print(pericana_table[pericana_table['sido']=='00'])

#2) 필요없는 값  ‘test’ 제거 
#print(pericana_table[pericana_table['sido']=='test'])
pericana_table = pericana_table.drop(pericana_table.index[1120])
pericana_table[pericana_table['sido']=='test']
#print(pericana_table[pericana_table['sido']=='test'])

#3)필요없는 값  ‘  ’ (공백) 제거
#print(pericana_table[pericana_table['sido']==' '])
pericana_table = pericana_table[pericana_table['sido'] != ' ']
#print(pericana_table.sido.unique())


"""행정구역 데이터(distric.csv)를 이용하여 행정구역 데이터 보정
#Pericana table에 있는 gungu와 distrivc.csv에 있는 gungu를 비교하면서 보정.
Sido table에는 없는 데이터를 찾으면 된다. Table.merge()로 찾는다.
판다스 테이블로 찾으면 데이터프레임이라 머지 함수 쓸 수 있다."""

sido_table = pd.read_csv("D:/Crawling_data/district.csv", encoding='CP949', index_col=0, header=0,engine='python' )
sido_table.head()

m = pericana_table.merge(sido_table, on=['sido', 'gungu'], how='outer', suffixes=['', '_'], indicator=True)
#print(m.head)


#4 행정구역 데이터(distric.csv)를 이용하여“left_only“를 제거
m_result = m.query('_merge=="left_only" ')
#print(m_result)


"""행정구역 데이터(distric.csv)를 이용하여 행정구역 데이터 보정
- 221개 데이터의 행정구역이 잘못되었으므로 보정한다.
→ 군구정보를 정정하기위한 딕셔너리를 만든 후에 적용시킴-(1)"""
#고칠것을 노가다로 찾아서 문자열로 저장해둔다. 딕셔너리에서 왼쪽은 key 오른쪽은 value

gungu_alias= """ 청주시흥덕구:청주시  여주군:여주시   청주시서원구:청주시  용인시기흥구:용인시   
고양시일산서구:고양시  부천시오정구:부천시  천안시동남구:천안시  부강면:세종시  연서면:세종시  
창원시진해구:창원시  나리로:세종시  갈매로:세종시  성남시수정구:성남시   청주시상당구:청주시  
전의면:세종시  조치원읍:세종시  전주시완산구:전주시  창원시마산합포구:창원시   당진군:당진시 
안산시단원구:안산시  부천시소사구:부천시  안산시상록구:안산시  수원시장안구:수원시 
고양시일산동구:고양시  천안시서북구:천안시  안양시동안구:안양시  부천시원미구:부천시  
전주시덕진구:전주시  포항시북구:포항시  창원시마산회원구:창원시   창원시성산구:창원시  
안양시만안구:안양시  포항시남구:포항시  """
#gungu_alias를 띄어쓰기 기준으로 잘라준다. 이걸 aliasset변수에 넣은 뒤에 :을 기준으로 분리해준다. 그럼 두 개로 분리가 됨
gungu_dict = dict(aliasset.split(':') for aliasset in gungu_alias.split())
#자료형을 dictionary로 변경해줌
#print(gungu_dict)
#람다함수는 간단한 함수(인라인 함수)로 "질문!!"
#lambda함수를 apply(적용)시킨다. 이건  pericana_table.gungu에 대해 적용되는거다. 이걸 다시 pericana_table.gungu에 넣어준다.
pericana_table.gungu = pericana_table.gungu.apply(lambda v: gungu_dict.get(v,v))
#수정한 pericana_table에서 다시 잘못된 행정구역 찾기
m = pericana_table.merge(sido_table, on= ['sido', 'gungu'], how='outer', suffixes=['', '_'], indicator=True) 
m_result = m.query('_merge =="left_only"')
#print(m_result)


#여기서 끝나면 좋겠지만 left_only가 남은걸 확인할 수 있음. 그냥 찾으면서 고치셈 ㅇㅇ


gungu_alias= """ 청주시흥덕구:청주시  여주군:여주시   청주시서원구:청주시  용인시기흥구:용인시   
고양시일산서구:고양시  부천시오정구:부천시  천안시동남구:천안시  부강면:세종시  연서면:세종시  
창원시진해구:창원시  나리로:세종시  갈매로:세종시  성남시수정구:성남시   청주시상당구:청주시  
전의면:세종시  조치원읍:세종시  전주시완산구:전주시  창원시마산합포구:창원시   당진군:당진시 
안산시단원구:안산시  부천시소사구:부천시  안산시상록구:안산시  수원시장안구:수원시 
고양시일산동구:고양시  천안시서북구:천안시  안양시동안구:안양시  부천시원미구:부천시  
전주시덕진구:전주시  포항시북구:포항시  창원시마산회원구:창원시   창원시성산구:창원시  
안양시만안구:안양시  포항시남구:포항시   수원시권선구:수원시   고양시덕양구:고양시 
청원군:청주시   용인시수지구:용인시   수원시영릉구:수원시  용인시처인구:용인시 
수원시팔달구:수원시   수원시영통구:수원시   성남시중원구:성남시   성남시분당구:성남시   청주시청원구:청주시   새롬북로:세종시"""


gungu_dict = dict(aliasset.split(':') for aliasset in gungu_alias.split())
pericana_table.gungu = pericana_table.gungu.apply(lambda v: gungu_dict.get(v,v))

#print(pericana_table)

m = pericana_table.merge(sido_table, on= ['sido', 'gungu'], how='outer', suffixes=['', '_'], indicator=True)
m_result = m.query('_merge =="left_only"')
#print(m_result)



#남구를 미추홀구로 바꿔야됨. 근데 인천광역시에만 남구가 있는게 아니라 인덱스로 시도가 인천광역시인것만 바꿔야됨
modified1=pericana_table[pericana_table['sido']=='인천광역시']
modified1=modified1[modified1['gungu']=='남구']
modified1.loc[modified1["gungu"] == "남구","gungu"] = "미추홀구"
#미추홀구로 바뀌는것까지는 잘 됐음. 이걸 m_result뒤에 붙어야된다.
modified1=pd.concat([pericana_table,modified1])
modified1=modified1.drop([962,963,964,965,966,967,1086],0)
#print(modified1)
"""     store sido gungu         store_address     _merge
1086  원주지사  테스트  테스트구  테스트 테스트구 테스트점 23-123  left_only""" #이것도 같이 제거해줌. 위에서.




#right_only 보정작업
m = modified1.merge(sido_table, on=['sido', 'gungu'], how='outer', suffixes=['', '_'], indicator=True)

#- 페리카나 주소 데이터에서 잘못된 행정구역을 찾아서 보정 : "right_only“를 제거
m_result = m.query('_merge=="right_only" ')
#print(m_result)
#store값이 Nan인 것이 모두 right_only이다.
m=m.drop([1114,1115,1116,1117,1118,1119,1120,1121,1122,1123,1124,1125,1126,1127,1128,1129,1130,1131,1132,1133,1134,1135,
                                       1136,1137,1138,1139,1140,1141,1142,1143,1144,1145,1146,1147,1148,1149,1150,1151,1152,1153,1154,1155,1156,
                                       1157,1158,1159,1160,1161,1162,1163,1164,1165,1166,1167,1168,1169,1170,1171,1172,1173,1174],0)
#print(m_result)
print(m)

#결과 테이블을 파일로 저장.


m.to_csv('D:/Crawling_data/pericana_modify.csv', encoding='CP949', mode='w', index=True)
