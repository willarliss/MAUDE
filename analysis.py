import sqlite3 as sql
import pandas as pd
import statsmodels.api as sm
from statsmodels.formula.api import ols
from statsmodels.stats.multicomp import pairwise_tukeyhsd
import matplotlib.pyplot as plt
import numpy as np



def read_sql(c):
    """Read in data from tables in external database"""
        
    # Pull product code and generic name
    c.execute("SELECT MDR_REPORT_KEY, DEVICE_REPORT_PRODUCT_CODE, GENERIC_NAME FROM devices")
    df1 = pd.DataFrame(c.fetchall(), columns=['MDR_REPORT_KEY', 
                                              'DEVICE_REPORT_PRODUCT_CODE', 
                                              'GENERIC_NAME'])
    
    # Pull manufacturer name
    c.execute("SELECT MDR_REPORT_KEY, MANUFACTURER_G1_NAME FROM master")
    df2 = pd.DataFrame(c.fetchall(), columns = ['MDR_REPORT_KEY',
                                                'MANUFACTURER_G1_NAME'])
    
    
    # Merge observations by report key
    df = pd.merge(df1, df2, on='MDR_REPORT_KEY')
    return df
    


def join_csv(df, f):
    """Read external csv file and merge to main dataframe"""
    
    # Load csv of death counts 
    csv = pd.read_csv(f, names=['MDR_REPORT_KEY', 'DEATHS'], dtype='str')
    # Merge death counts with main dataframe by report key
    df_merged = pd.merge(df, csv, on='MDR_REPORT_KEY')
    
    return df_merged



def key(x):
    """Key for product codes"""
    
    # Read in key file
    df = pd.read_csv('device_id_web_info.txt', delimiter='|', dtype='str')
    
    # Create key dictionary
    key = {}
    for c,n in zip(df['productCode'], df['productCodeName']):
        key[c] = n
    
    # Return product name if in key
    try:
        X = key[x]
    except KeyError:
        X = x
        
    return X



def tally(df, grouping, col='DEATHS', n=10, ret=False):
    """Count total number of deaths from grouped by specific variable"""

    # Create new object of total deaths by group sorted from greatest to least   
    df2 = pd.DataFrame({grouping: [g for g in df[grouping] if g != '*'], 
                        col: [int(c) for g,c in zip(df[grouping], df[col]) if g != '*']})
    X = dict(df2.groupby(grouping)[col].sum())
    X = sorted(X.items(), key=lambda x: x[1], reverse=True)
    
    top = []
    count = 1
    # Report numbers
    for x in X:
        print(str(count)+':', key(x[0]), '---', x[1])
        top.append(x)

        # Only report amount specified
        if count == n:
            break
        else:
            count += 1
    
    # Return list of top group classes
    if ret == True:
        return top
    
    
    
def most(df, grouping, col='DEATHS', n=10):
    """Report maximum number of deaths for any column grouping"""
    
    # Calculate and sort list of maximum death counts grouped by given column
    df[col] = [int(i) for i in df[col]]
    X = dict(df.groupby(grouping)[col].max())
    X = sorted(X.items(), key=lambda x: x[1], reverse=True)
    
    count = 1
    # Report numbers
    for x in X: 
        print(str(count)+':', key(x[0]), '---', x[1])
    
        # Only report amount specified
        if count == n:
            break
        else:
            count += 1
    
    

def aov(df, treatment, col='DEATHS', repeat=False):
    """Analysis of Variance"""
    
    # Create object of treatment and column excluding NoneTypes
    dta = {treatment:[], col:[]}
    for c,t in zip(df[col], df[treatment]):
        if c != None and t != None:
            dta[col].append(int(c))
            dta[treatment].append(t)
    dta = pd.DataFrame(dta, columns=[col, treatment])
    
    # If repeat is not specified, report one ANOVA table
    if repeat == False:
        # Data is too large to run ANOVA on full set. Pull sample of 0.001 size
        dta_sample = dta.sample(frac=0.001)
        mod = ols(f'{col} ~ {treatment}', data=dta_sample).fit()
        try:
            aov_table = sm.stats.anova_lm(mod, typ=2)
            print('n = '+str(len(dta_sample)), aov_table, sep='\n')
        # Avoid "LinAlgError: SVD did not converge" exception
        except:
            print('PASS')
    
    # If repeat is specified, report 10 ANOVAs
    elif repeat == True:
        for r in range(10):
            # Data is too large to run ANOVA on full set. Pull sample of 0.001 size
            # Pull new sample for every iteration
            dta_sample = dta.sample(frac=0.001)
            mod = ols(f'{col} ~ {treatment}', data=dta_sample).fit()
            try:
                aov_table = sm.stats.anova_lm(mod, typ=2)
                num = r + 1
                print(f'sample: {num}', aov_table, ' ', sep='\n')
            # Avoid "LinAlgError: SVD did not converge" exception
            except:
                print('PASS')
            
            

def aov_tops(df, treatment, names, col='DEATHS', repeat=False):
    """Analysis of Variance for specified group classifications"""
    
    # Create object of treatment and column excluding NoneTypes
    # Only pull treatment classifiers that are in specified list
    names = [i[0] for i in names]
    dta = {treatment:[], col:[]}
    for c,t in zip(df[col], df[treatment]):
        if c != None and t != None:
            if t in names:
                dta[col].append(int(c))
                dta[treatment].append(t)
    dta = pd.DataFrame(dta, columns=[col, treatment])

    # If repeat is not specified, report one ANOVA table    
    if repeat == False:
        # Data is too large to run ANOVA on full set. Pull sample of 0.01 size
        dta_sample = dta.sample(frac=0.01)
        mod = ols(f'{col} ~ {treatment}', data=dta_sample).fit()
        try:
            aov_table = sm.stats.anova_lm(mod, typ=2)
            print('n = '+str(len(dta_sample)), aov_table, sep='\n')
        # Avoid "LinAlgError: SVD did not converge" exception
        except:
            print('PASS')
            
    # If repeat is specified, report 10 ANOVAs
    elif repeat == True:
        for r in range(10):
            # Data is too large to run ANOVA on full set. Pull sample of 0.01 size
            # Pull new sample for every iteration            
            dta_sample = dta.sample(frac=0.01)
            mod = ols(f'{col} ~ {treatment}', data=dta_sample).fit()
            try:
                aov_table = sm.stats.anova_lm(mod, typ=2)
                num = r + 1
                print(f'sample: {num}', aov_table, ' ', sep='\n')
            # Avoid "LinAlgError: SVD did not converge" exception
            except:
                print('PASS')
            


def hsd(df, treatment, names, col='DEATHS'):
    """Tukey's Honest Student Difference test"""
    
    # Create object of treatment and column excluding NoneTypes
    # Only pull treatment classifiers that are in specified list    
    names = [i[0] for i in names]
    dta = {treatment:[], col:[]}
    for c,t in zip(df[col], df[treatment]):
        if c != None and t != None:
            if t in names:
                dta[col].append(int(c))
                dta[treatment].append(t)
    dta = pd.DataFrame(dta, columns=[col, treatment])
    
    # Listwise comparison of means of each item in specified list
    res = pairwise_tukeyhsd(dta[col], dta[treatment])
    print(res) 
    
    

def ecdf(df, var):
    """ECDF plot of given variable"""
    
    x = sorted([i for i in df[var] if i != None])
    y = [i/len(x) for i in range(len(x))]
    
    plt.figure()
    plt.plot(x, y, '.')
    plt.ylabel('Pr(>y)')
    plt.xlabel(var)
    plt.title('ECDF')



def box(df, names, grouping, col='DEATHS'):
    """Boxplot of given variable and specified groups"""
        
    dta = []
    # Format list of grouped observations for plotter
    for name in names:
        d = [int(c) for g,c in zip(df[grouping], df[col]) if g == name[0] and c != None]
        dta.append(np.array(d))

    plt.figure()
    plt.boxplot(dta, 'k')
    plt.ylabel(f'{col} Count')
    plt.xlabel(grouping)
    plt.title('Box Plot')
    
    # Plot key
    n, k = 0, 1
    for name in names:
        txt = str(k) + ': ' + key(name[0])
        plt.text(.5, -3-n, txt)
        n += .5
        k += 1   

        

if __name__ == '__main__':
    
#    connection = sql.connect('data.db')
#    con = connection.cursor()
#    try:    
#        _df = read_sql(con)
#    except Exception as e:
#        print('EXCEPTION', e)
#    connection.close()
    
#    df_main = join_csv(_df, 'body_count.csv')
#    del _df
      
#    tally(df_main, grouping='DEVICE_REPORT_PRODUCT_CODE', col='DEATHS', n=10, ret=False)
#    tally(df_main, grouping='MANUFACTURER_G1_NAME', col='DEATHS', n=10, ret=False)
#    tally(df_main, grouping='GENERIC_NAME', col='DEATHS', n=10, ret=False)
#    
#    aov(df_main, treatment='MANUFACTURER_G1_NAME', col='DEATHS', repeat=True)
#    aov(df_main, treatment='GENERIC_NAME', col='DEATHS', repeat=True)
#    aov(df_main, treatment='DEVICE_REPORT_PRODUCT_CODE', col='DEATHS', repeat=True)

#    most(df_main, grouping='DEVICE_REPORT_PRODUCT_CODE', col='DEATHS', n=3)
#    most(df_main, grouping='GENERIC_NAME', col='DEATHS', n=3)
#    most(df_main, grouping='MANUFACTURER_G1_NAME', col='DEATHS', n=3)
    
#    ecdf(df_main, var='DEATHS')
#    box(df_main, grouping='DEVICE_REPORT_PRODUCT_CODE', col='DEATHS', names=top_prod)

###    

    aov(df_main, treatment='MANUFACTURER_G1_NAME', col='DEATHS', repeat=True)
    print()
    top_prod = tally(df_main, grouping='MANUFACTURER_G1_NAME', col='DEATHS', n=5, ret=True)
    print()
    aov_tops(df_main, treatment='MANUFACTURER_G1_NAME', col='DEATHS', names=top_prod, repeat=False)
    print()
    hsd(df_main, treatment='MANUFACTURER_G1_NAME', names=top_prod, col='DEATHS')
    print()
    box(df_main, grouping='MANUFACTURER_G1_NAME', col='DEATHS', names=top_prod)


    
    
    
    
    