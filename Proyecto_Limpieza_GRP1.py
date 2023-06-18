#!/usr/bin/env python
# coding: utf-8

# In[44]:


#Mildred L.L.
import pandas as pd


# In[45]:


#CARGAR DATOS
def carga_datos(año_inicial, año_final):
    nuevas_columnas = ['Zona','Provincia', 'Canton', 'Parroquia','Nombre_Institucion','Promovidos','NoPromovidos','Abandono','Cla_Niv_Edu','Clave_periodo','Clave_primaria','AMIE']
    lista_dataframes = []

    for año in range(año_inicial, año_final+1):
        archivo = f'C://Users//Mil//Documents//Proyecto Fin Maestría//CSV//Archivos//MINEDUC_Registros_{año}-{año+1}.csv'       
        df = pd.read_csv(archivo, sep=';')

        for columna in nuevas_columnas:
            compl = f'C://Users//Mil//Documents//Proyecto Fin Maestría//CSV//Archivos//MINEDUC_Administrativo_{año}-{año+1}.xlsx'
            dfinst = pd.read_excel(compl, 'Cod_Institucion')
                        
            if archivo != 'C://Users//Mil//Documents//Proyecto Fin Maestría//CSV//Archivos//MINEDUC_Registros_2021-2022.csv':
                map_dict1 = pd.Series(dfinst['Nombre_Institucion'].values, index=dfinst['Codigo_Institucion']).to_dict()
                df['Nombre_Institucion'] = df['Codigo_Institucion'].map(map_dict1)

            else:
                map_dict1 = pd.Series(dfinst['Codigo_Institucion'].values, index=dfinst['Nombre_Institucion']).to_dict()
                df['Codigo_Institucion'] = df['Nombre_Institucion'].map(map_dict1)
 
            
            df['Clave_periodo'] = str(año) + '-' + str(año+1)
            df['Clave_primaria'] = df['Codigo_Institucion'] + df['Clave_periodo']
  
            dfprov = pd.read_excel(compl, 'Cod_Provincia')
            map_dict2 = pd.Series(dfprov['Provincia'].values, index=dfprov['Cod_Provincia']).to_dict()
            df['Provincia'] = df['Cod_Provincia'].map(map_dict2)

            dfcant = pd.read_excel(compl, 'Cod_Canton')
            map_dict3 = pd.Series(dfcant['Canton'].values, index=dfcant['Cod_Canton']).to_dict()
            df['Canton'] = df['Cod_Canton'].map(map_dict3)
            
            dfparr = pd.read_excel(compl, 'Cod_Parroquia')
            map_dict4 = pd.Series(dfparr['Parroquia'].values, index=dfparr['Cod_Parroquia']).to_dict()
            df['Parroquia'] = df['Cod_Parroquia'].map(map_dict4)
            
            
            #Métricas
            df = calc_met_est(df)
                        
            #carga_complementarios()
            compl = 'C://Users//Mil//Documents//Proyecto Fin Maestría//CSV//Archivos//Registros_Complementarios_2009-2022.csv'
            amie = 'C://Users//Mil//Documents//Proyecto Fin Maestría//CSV//Archivos//Registro_AMIE_2009-2022.csv'

            dfcomp = pd.read_csv(compl, sep=';', low_memory=False)
            dfamie = pd.read_csv(amie, sep=';', low_memory=False)
            
            map_dicta = pd.Series(dfamie['AMIE'].values, index=dfamie['Clave_primaria']).to_dict()
            df['AMIE'] = df['Clave_primaria'].map(map_dicta)
            
  
            #Nacionalidades
            
            nacionalidad_to_map = ['Ecuatoriana', 'Colombiana', 'Venezolana', 'Peruana', 'Otros_Paises_de_America', 'Otros_Continentes']

            for columna in nacionalidad_to_map:
                map_dictna = pd.Series(dfcomp[columna].values, index=dfcomp['Clave_primaria']).to_dict()
                df[columna] = df['Clave_primaria'].map(map_dictna)    
            
            

            #Discapacidad
            
            columns = ['Estudiantes_con_discapacidad', 'Estudiantes_sin_discapacidad', 'Docentes_con_discapacidad', 'Docentes_sin_discapacidad']

            for column in columns:
                map_dict = pd.Series(dfcomp[column].values, index=dfcomp['Clave_primaria']).to_dict()
                df[column] = df['Clave_primaria'].map(map_dict) 
            
            
            
            
            #Etnias
            
            etnia_est = ['EMestiza', 'EIndigena', 'EMontubio', 'Eafroecuatoriana', 'EBlanca']

            for columneest in etnia_est:
                map_dicta = pd.Series(dfcomp[columneest].values, index=dfcomp['Clave_primaria']).to_dict()
                df[columneest] = df['Clave_primaria'].map(map_dicta)
                

            etnia_doc = ['DMestiza', 'DIndigena', 'DMontubio', 'Dafroecuatoriana', 'DBlanca']

            for columnedoc in etnia_doc:
                map_dicta = pd.Series(dfcomp[columnedoc].values, index=dfcomp['Clave_primaria']).to_dict()
                df[columnedoc] = df['Clave_primaria'].map(map_dicta)
            
            
            
            #otros
            
            col_otros = ['Aceso_a_Internet', 'Contrato', 'Nombramiento', 'Otro_tipo_de_relación_laboral']

            for columnot in col_otros:
                map_dictot = pd.Series(dfcomp[columnot].values, index=dfcomp['Clave_primaria']).to_dict()
                df[columnot] = df['Clave_primaria'].map(map_dictot)
            
            if columna not in df.columns:
                df[columna] = None
                       
        lista_dataframes.append(df)

    datos_mineduc = pd.concat(lista_dataframes, ignore_index=True)
    
    condicion_nivel = datos_mineduc['Cla_Niv_Edu'].isin(['EGB', 'EGB y BGU', 'BGU'])
    datos_mineduc = datos_mineduc.loc[condicion_nivel]
    
    condicion_provincia = datos_mineduc['Provincia'].isin(['PICHINCHA', 'GUAYAS'])
    datos_mineduc = datos_mineduc.loc[condicion_provincia]    

    return datos_mineduc

datos_mineduc = carga_datos(2017, 2021)


# In[ ]:





# In[46]:


# EXPORTAR ARCHIVO EXCEL
ruta_archivo = 'C://Users//Mil//Documents//Proyecto Fin Maestría//CSV//Archivos//Datos_2017-2022_mejorado.xlsx'
datos_mineduc.to_excel(ruta_archivo, sheet_name='MINEDUC', index=False)


# In[41]:


def calc_met_est(df):
    promovidos_egb_cols = ['EstudiantesFemeninoPromovidosPrimerAñoEGB', 'EstudiantesMasculinoPromovidosPrimerAñoEGB',
                              'EstudiantesFemeninoPromovidosSegundoAñoEGB', 'EstudiantesMasculinoPromovidosSegundoAñoEGB',
                              'EstudiantesFemeninoPromovidosTercerAñoEGB', 'EstudiantesMasculinoPromovidosTercerAñoEGB',
                              'EstudiantesFemeninoPromovidosCuartoAñoEGB', 'EstudiantesMasculinoPromovidosCuartoAñoEGB',
                              'EstudiantesFemeninoPromovidosQuintoAñoEGB', 'EstudiantesMasculinoPromovidosQuintoAñoEGB',
                              'EstudiantesFemeninoPromovidosSextoAñoEGB', 'EstudiantesMasculinoPromovidosSextoAñoEGB',
                              'EstudiantesFemeninoPromovidosSeptimoAñoEGB', 'EstudiantesMasculinoPromovidosSeptimoAñoEGB',
                              'EstudiantesFemeninoPromovidosOctavoAñoEGB', 'EstudiantesMasculinoPromovidosOctavoAñoEGB',
                              'EstudiantesFemeninoPromovidosNovenoAñoEGB', 'EstudiantesMasculinoPromovidosNovenoAñoEGB',
                              'EstudiantesFemeninoPromovidosDecimoAñoEGB', 'EstudiantesMasculinoPromovidosDecimoAñoEGB']
    

    promovidos_bgu_cols = ['EstudiantesFemeninoPromovidosPrimerAñoBACH', 'EstudiantesMasculinoPromovidosPrimerAñoBACH',
                              'EstudiantesFemeninoPromovidosSegundoAñoBACH', 'EstudiantesMasculinoPromovidosSegundoAñoBACH',
                              'EstudiantesFemeninoPromovidosTercerAñoBACH', 'EstudiantesMasculinoPromovidosTercerAñoBACH']
    


    no_promovidos_egb_cols = ['EstudiantesFemeninoNoPromovidosPrimerAñoEGB', 'EstudiantesMasculinoNoPromovidosPrimerAñoEGB',
                                 'EstudiantesFemeninoNoPromovidosSegundoAñoEGB', 'EstudiantesMasculinoNoPromovidosSegundoAñoEGB',
                                 'EstudiantesFemeninoNoPromovidosTercerAñoEGB', 'EstudiantesMasculinoNoPromovidosTercerAñoEGB',
                                 'EstudiantesFemeninoNoPromovidosCuartoAñoEGB', 'EstudiantesMasculinoNoPromovidosCuartoAñoEGB',
                                 'EstudiantesFemeninoNoPromovidosQuintoAñoEGB', 'EstudiantesMasculinoNoPromovidosQuintoAñoEGB',
                                 'EstudiantesFemeninoNoPromovidosSextoAñoEGB', 'EstudiantesMasculinoNoPromovidosSextoAñoEGB',
                                 'EstudiantesFemeninoNoPromovidosSeptimoAñoEGB', 'EstudiantesMasculinoNoPromovidosSeptimoAñoEGB',
                                 'EstudiantesFemeninoNoPromovidosOctavoAñoEGB', 'EstudiantesMasculinoNoPromovidosOctavoAñoEGB',
                                 'EstudiantesFemeninoNoPromovidosNovenoAñoEGB', 'EstudiantesMasculinoNoPromovidosNovenoAñoEGB',
                                 'EstudiantesFemeninoNoPromovidosDecimoAñoEGB', 'EstudiantesMasculinoNoPromovidosDecimoAñoEGB']

    no_promovidos_bgu_cols = ['EstudiantesFemeninoNoPromovidosPrimerAñoBACH', 'EstudiantesMasculinoNoPromovidosPrimerAñoBACH',
                                 'EstudiantesFemeninoNoPromovidosSegundoAñoBACH', 'EstudiantesMasculinoNoPromovidosSegundoAñoBACH',
                                 'EstudiantesFemeninoNoPromovidosTercerAñoBACH', 'EstudiantesMasculinoNoPromovidosTercerAñoBACH']

    abandono_egb = ['EstudiantesFemeninoDesertoresPrimerAñoEGB', 'EstudiantesMasculinoDesertoresPrimerAñoEGB',
                            'EstudiantesFemeninoDesertoresSegundoAñoEGB', 'EstudiantesMasculinoDesertoresSegundoAñoEGB',
                            'EstudiantesFemeninoDesertoresTercerAñoEGB', 'EstudiantesMasculinoDesertoresTercerAñoEGB',
                            'EstudiantesFemeninoDesertoresCuartoAñoEGB', 'EstudiantesMasculinoDesertoresCuartoAñoEGB',
                            'EstudiantesFemeninoDesertoresQuintoAñoEGB', 'EstudiantesMasculinoDesertoresQuintoAñoEGB',
                            'EstudiantesFemeninoDesertoresSextoAñoEGB', 'EstudiantesMasculinoDesertoresSextoAñoEGB',
                            'EstudiantesFemeninoDesertoresSeptimoAñoEGB', 'EstudiantesMasculinoDesertoresSeptimoAñoEGB',
                            'EstudiantesFemeninoDesertoresOctavoAñoEGB', 'EstudiantesMasculinoDesertoresOctavoAñoEGB',
                            'EstudiantesFemeninoDesertoresNovenoAñoEGB', 'EstudiantesMasculinoDesertoresNovenoAñoEGB',
                            'EstudiantesFemeninoDesertoresDecimoAñoEGB', 'EstudiantesMasculinoDesertoresDecimoAñoEGB']

    abandono_bgu = ['EstudiantesFemeninoDesertoresPrimerAñoBACH', 'EstudiantesMasculinoDesertoresPrimerAñoBACH',
                            'EstudiantesFemeninoDesertoresSegundoAñoBACH', 'EstudiantesMasculinoDesertoresSegundoAñoBACH',
                            'EstudiantesFemeninoDesertoresTercerAñoBACH', 'EstudiantesMasculinoDesertoresTercerAñoBACH']

    df['PromovidosEGB'] = df[promovidos_egb_cols].sum(axis=1)
    df['PromovidosBGU'] = df[promovidos_bgu_cols].sum(axis=1)
    df['Promovidos'] = df['PromovidosEGB'] + df['PromovidosBGU']

    df['NoPromovidosEGB'] = df[no_promovidos_egb_cols].sum(axis=1)
    df['NoPromovidosBGU'] = df[no_promovidos_bgu_cols].sum(axis=1)
    df['NoPromovidos'] = df['NoPromovidosEGB'] + df['NoPromovidosBGU']

    df['AbandonoEGB'] = df[abandono_egb].sum(axis=1)
    df['AbandonoBGU'] = df[abandono_bgu].sum(axis=1)
    df['Abandono'] = df['AbandonoEGB'] + df['AbandonoBGU']
    
    df['Tot_Estudiantes_EGB'] = df['PromovidosEGB'] + df['NoPromovidosEGB'] + df['AbandonoEGB']
    df['Tot_Estudiantes_BGU'] = df['PromovidosBGU'] + df['NoPromovidosBGU'] + df['AbandonoBGU']
    df['Tot_Estudiantes'] = df['Promovidos'] + df['NoPromovidos'] + df['Abandono']
    
    df['1EGB'] = df['EstudiantesFemeninoPromovidosPrimerAñoEGB']+df['EstudiantesMasculinoPromovidosPrimerAñoEGB']+df['EstudiantesFemeninoNoPromovidosPrimerAñoEGB'] + df['EstudiantesMasculinoNoPromovidosPrimerAñoEGB']+df['EstudiantesFemeninoDesertoresPrimerAñoEGB'] + df['EstudiantesMasculinoDesertoresPrimerAñoEGB']
    df['2EGB'] = df['EstudiantesFemeninoPromovidosSegundoAñoEGB']+df['EstudiantesMasculinoPromovidosSegundoAñoEGB']+df['EstudiantesFemeninoNoPromovidosSegundoAñoEGB'] + df['EstudiantesMasculinoNoPromovidosSegundoAñoEGB']+df['EstudiantesFemeninoDesertoresSegundoAñoEGB'] + df['EstudiantesMasculinoDesertoresSegundoAñoEGB']
    df['3EGB'] = df['EstudiantesFemeninoPromovidosTercerAñoEGB']+df['EstudiantesMasculinoPromovidosTercerAñoEGB']+df['EstudiantesFemeninoNoPromovidosTercerAñoEGB'] + df['EstudiantesMasculinoNoPromovidosTercerAñoEGB']+df['EstudiantesFemeninoDesertoresTercerAñoEGB'] + df['EstudiantesMasculinoDesertoresTercerAñoEGB']
    df['4EGB'] = df['EstudiantesFemeninoPromovidosCuartoAñoEGB']+df['EstudiantesMasculinoPromovidosCuartoAñoEGB']+df['EstudiantesFemeninoNoPromovidosCuartoAñoEGB'] + df['EstudiantesMasculinoNoPromovidosCuartoAñoEGB']+df['EstudiantesFemeninoDesertoresCuartoAñoEGB'] + df['EstudiantesMasculinoDesertoresCuartoAñoEGB']
    df['5EGB'] = df['EstudiantesFemeninoPromovidosQuintoAñoEGB']+df['EstudiantesMasculinoPromovidosQuintoAñoEGB']+df['EstudiantesFemeninoNoPromovidosQuintoAñoEGB'] + df['EstudiantesMasculinoNoPromovidosQuintoAñoEGB']+df['EstudiantesFemeninoDesertoresQuintoAñoEGB'] + df['EstudiantesMasculinoDesertoresQuintoAñoEGB']
    df['6EGB'] = df['EstudiantesFemeninoPromovidosSextoAñoEGB']+df['EstudiantesMasculinoPromovidosSextoAñoEGB']+df['EstudiantesFemeninoNoPromovidosSextoAñoEGB'] + df['EstudiantesMasculinoNoPromovidosSextoAñoEGB']+df['EstudiantesFemeninoDesertoresSextoAñoEGB'] + df['EstudiantesMasculinoDesertoresSextoAñoEGB']
    df['7EGB'] = df['EstudiantesFemeninoPromovidosSeptimoAñoEGB']+df['EstudiantesMasculinoPromovidosSeptimoAñoEGB']+df['EstudiantesFemeninoNoPromovidosSeptimoAñoEGB'] + df['EstudiantesMasculinoNoPromovidosSeptimoAñoEGB']+df['EstudiantesFemeninoDesertoresSeptimoAñoEGB'] + df['EstudiantesMasculinoDesertoresSeptimoAñoEGB']
    df['8EGB'] = df['EstudiantesFemeninoPromovidosOctavoAñoEGB']+df['EstudiantesMasculinoPromovidosOctavoAñoEGB']+df['EstudiantesFemeninoNoPromovidosOctavoAñoEGB'] + df['EstudiantesMasculinoNoPromovidosOctavoAñoEGB']+df['EstudiantesFemeninoDesertoresOctavoAñoEGB'] + df['EstudiantesMasculinoDesertoresOctavoAñoEGB']
    df['9EGB'] = df['EstudiantesFemeninoPromovidosNovenoAñoEGB']+df['EstudiantesMasculinoPromovidosNovenoAñoEGB']+df['EstudiantesFemeninoNoPromovidosNovenoAñoEGB'] + df['EstudiantesMasculinoNoPromovidosNovenoAñoEGB']+df['EstudiantesFemeninoDesertoresNovenoAñoEGB'] + df['EstudiantesMasculinoDesertoresNovenoAñoEGB']
    df['10EGB'] = df['EstudiantesFemeninoPromovidosDecimoAñoEGB']+df['EstudiantesMasculinoPromovidosDecimoAñoEGB']+df['EstudiantesFemeninoNoPromovidosDecimoAñoEGB'] + df['EstudiantesMasculinoNoPromovidosDecimoAñoEGB']+df['EstudiantesFemeninoDesertoresDecimoAñoEGB'] + df['EstudiantesMasculinoDesertoresDecimoAñoEGB']

    df['1BGU'] = df['EstudiantesFemeninoPromovidosPrimerAñoBACH']+df['EstudiantesMasculinoPromovidosPrimerAñoBACH']+df['EstudiantesFemeninoNoPromovidosPrimerAñoBACH'] + df['EstudiantesMasculinoNoPromovidosPrimerAñoBACH']+df['EstudiantesFemeninoDesertoresPrimerAñoBACH'] + df['EstudiantesMasculinoDesertoresPrimerAñoBACH']
    df['2BGU'] = df['EstudiantesFemeninoPromovidosSegundoAñoBACH']+df['EstudiantesMasculinoPromovidosSegundoAñoBACH']+df['EstudiantesFemeninoNoPromovidosSegundoAñoBACH'] + df['EstudiantesMasculinoNoPromovidosSegundoAñoBACH']+df['EstudiantesFemeninoDesertoresSegundoAñoBACH'] + df['EstudiantesMasculinoDesertoresSegundoAñoBACH']
    df['3BGU'] = df['EstudiantesFemeninoPromovidosTercerAñoBACH']+df['EstudiantesMasculinoPromovidosTercerAñoBACH']+df['EstudiantesFemeninoNoPromovidosTercerAñoBACH'] + df['EstudiantesMasculinoNoPromovidosTercerAñoBACH']+df['EstudiantesFemeninoDesertoresTercerAñoBACH'] + df['EstudiantesMasculinoDesertoresTercerAñoBACH']

    

    df['Cla_Niv_Edu'] = df.apply(clasifica_nivedu, axis=1)
    df['Cla_Area'] = df.apply(clasifica_area, axis=1)
    
    return df


# In[42]:


def clasifica_nivedu(fila):
        
            mapeo_niv = {
                'Inicial y EGB': 'EGB',
                'Inicial, Educación Básica': 'EGB',
                'Educación Básica': 'EGB',
                'Educación Básica y Artesanal P.P': 'EGB',
                'Educación Básica y Alfabetización P.P.': 'EGB',
                'Educación Básica y Alfabetización': 'EGB',
                'Educación Básica, Alfabetización y Artesanal P.P.': 'EGB',
                'Inicial y Bachillerato': 'BGU',
                'Bachillerato': 'BGU',
                'Bachillerato y Artesanal P.P.': 'BGU',
                'Bachillerato y Alfabetización P.P.': 'BGU',
                'EGB y Bachillerato': 'EGB y BGU',
                'Educación Básica y Bachillerato': 'EGB y BGU',
                'Inicial, Educación Básica y Bachillerato': 'EGB y BGU',
                'Educación Básica, Bachillerato y Artesanal P.P.': 'EGB y BGU',
                'Educación Básica,Bachillerato y Artesanal P.P': 'EGB y BGU',
                'Educación Básica, Bachillerato y Alfabetización P.P.': 'EGB y BGU',
                'Educación Básica, Bachillerato,Alfabetización y Artesanal P.P.': 'EGB y BGU'
            }

            return mapeo_niv.get(fila['Nivel_Educacion'], fila['Nivel_Educacion'])
        
        
     


# In[43]:


def clasifica_area(fila1):
            
            mapeo_area = {
                'RuralINEC': 'Rural',
                'UrbanaINEC': 'Urbana'
            }

            return mapeo_area.get(fila1['Area'], fila1['Area'])   


# In[ ]:





# In[ ]:





# In[ ]:



    


# In[ ]:


#COMPROBACIONES
datos_mineduc.info()


# In[ ]:


#100 últimos registros
datos_mineduc.tail(100)


# In[ ]:


#registros no vacíos
df_no_vacios = datos_mineduc[datos_mineduc['Clave_primaria'].notnull()]
print(df_no_vacios)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




