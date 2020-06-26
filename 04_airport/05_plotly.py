import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

df=pd.read_csv('avg_delay_arrival.csv',sep='\t',names=['month','to_split'])
df[['departure_delay','arrival_delay']]=df.to_split.str[1:-1].str.split(', ').apply(pd.Series)
df=df.drop(['to_split'],axis=1)
#print(df.head())

fig=px.bar(df,x='month',y='departure_delay',color='month')
fig=go.Figure(data=[go.Bar(x=df.month,y=df.departute_delay),go.Bar(x=df.month,y=df.arrival_delay)])