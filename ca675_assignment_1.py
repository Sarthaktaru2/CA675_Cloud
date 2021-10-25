#!/usr/bin/env python
# coding: utf-8

# <b><h3>CA675 Cloud Assignment</h3></b><br><br>
# Name - Sarthak Bhagwat Taru<br>
# StudentID - 21261303<br>
# Email - sarthak.taru2@mail.dcu.ie
# 

# <br><br>
# <b>Task 1 - Acquire the top 2,00,000 posts by ViewCount</b><br><br>
# 
# Query executed on stackexchange data platform to retrieve the top 2,00,000 posts by viewcount https://data.stackexchange.com/stackoverflow/query/new<br><br>
# 
# <b>SQL Query-</b><br>
#         SELECT * FROM 
#         (
#           SELECT ps.Id,
#             ps.PostTypeId,
#             ps.ViewCount,
#             ps.Title,
#             ps.AcceptedAnswerId,
#             ps.ParentId,
#             ps.CreationDate,
#             ps.DeletionDate,
#             ps.Score,
#             us.DisplayName,
#             us.EmailHash,
#             ps.LastEditorDisplayName,
#             ps.Tags,
#             ps.ContentLicense,
#             ps.OwnerUserId,
#             ps.FavoriteCount,
#             ps.AnswerCount,
#             ps.CommentCount,
#             ROW_NUMBER() OVER (ORDER BY ViewCount DESC) as row_num
#             FROM Posts ps
#             inner join
#             Users as us
#             on us.Id=ps.OwnerUserId
#         )tab Where row_num BETWEEN 150001 AND 200000<br>
# 
# Above sql returns the 50000 records per execution hence fired it for 4 times to extract 2,00,000 post. Then the data is extracted into 4 csv files. <br><br>
# 
# <b>Task 2 & 3 - Use Pig/Hive/MapReduce - Extract, Transform and Load the data as applicable to get:</b><br>
#     This assignment is completed on Google Cloud Platform by creating the cluster with Dataproc service. Cluster was configured with Jupyter notebook execute python code. I have used the Hive because of the prior hands-on experience in sql query language.<br>
#     Created the ca675 Database and Table creation is as follows -<br>
#     <b>sql query-</b>CREATE TABLE IF NOT EXISTS ca675.Stackexchange (Id int, PostTypeId int, ViewCount int, Title varchar(255), AcceptedAnswerId int, ParentId int, CreationDate timestamp, DeletionDate timestamp, Score int, OwnerDisplayName varchar(255), EmailHash varchar(255), LastEditorDisplayName varchar(255), Tags varchar(255), ContentLicense varchar(255), OwnerUserId int, FavoriteCount int, AnswerCount int, CommentCount int, row_num int) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde';<br>
#     
#    Data loading into hive - 
#     Uploaded the csv file in dataproc cloud storage and loaded the data through below sql query<br>
#     LOAD DATA INPATH 'gs://dataproc-staging-us-central1-1078341724532-t5a0gorr/combined.csv' INTO TABLE ca675.Stackexchange;<br>
#     
#     Below code is written in the Jupyter notebook accessed by public address created by cluster.
# 
# 

# In[1]:


from pyhive import hive
from tabulate import tabulate
import pandas as pd


# In[4]:


host_name = "localhost"
port = 10000
user = "sarthak_taru2"
password = "1988377416667072609"
database="ca675"
def hiveconnection(host_name, port, user,password, database):
    conn = hive.Connection(host=host_name, port=port,username=user,password=password,database=database,auth='CUSTOM')
    return conn
conn = hiveconnection(host_name, port, user,password, database)
cur = conn.cursor()


# <br><b>Task 2.1 - The top 10 posts by score</b><br>

# In[6]:


cur.execute('''select Id,OwnerUserId,OwnerDisplayName,score,viewcount
FROM stackexchanges
order by score desc LIMIT 10''')
result = cur.fetchall()
df = pd.DataFrame (result, columns = ['Id','OwnerUserId','OwnerDisplayName','Score','Viewcount'])
display(df)


# <br><b>Task 2.2 - The top 10 users by post score</b><br>

# In[10]:


cur.execute('''
select OwnerUserId,max(OwnerDisplayName) , sum(score) as score
from stackexchanges
group by OwnerUserId
order by score desc limit 10
''')
result = cur.fetchall()
df1 = pd.DataFrame (result, columns = ['OwnerUserId','OwnerDisplayName','score'])
display(df1)


# <br><b>Task 3 - The number of distinct users, who used the word “cloud” in one of their
# posts</b><br>
# Here I have considered the Title and Tags to find out the total count of owner user id who used the word cloud in one of their posts.

# In[11]:


cur.execute('''
SELECT
COUNT(DISTINCT OwnerUserId) as owner_user_count
FROM stackexchanges
WHERE Title LIKE '%cloud%' or tags LIKE '%cloud%'
''')
result = cur.fetchall()
df2 = pd.DataFrame (result, columns = ['owner_user_count'])
display(df2)


# <br><b>Task 4 - calculate the per-user TF-IDF of the top 10 terms for each of the top 10 users</b><br>
# So first of all, I have extracted the top 10 owner user id with their titles and created the list of top 10 users.

# In[18]:


df3 = pd.read_sql("""
        SELECT OwnerUserId,OwnerDisplayName,Title
        from stackexchanges
        WHERE OwnerUserId
        IN
        (
            select OwnerUserId from(select OwnerUserId,max(OwnerDisplayName) , sum(score) as score
            from stackexchanges
            group by OwnerUserId
            order by score desc limit 10)stack 
        )
        order by OwnerUserId""", conn)
        
result = cur.fetchall()
top_10_users = list(df3["owneruserid"].unique())
display(top_10_users)


# <br>I have taken the reference from below site to implement tf-idf<br>https://scikitlearn.org/stable/modules/generated/sklearn.feature_extraction.text.TfidfVectorizer.html<br>
# 
# for each of the owner and their top 10 words plot the table accordingly.

# In[26]:


from sklearn.feature_extraction.text import TfidfVectorizer

def calculate_tfidf(title_each_user):
    vectorizer = TfidfVectorizer(stop_words='english',lowercase=True)
    response = vectorizer.fit_transform(title_each_user['title'])
    df_tfidf_words = pd.DataFrame(response.toarray(),columns=vectorizer.get_feature_names()) #calculating tf-idf values
    df_final_result_per_user=df_tfidf_words.sum(axis=0, numeric_only= True) #suming up the tf-idf to get the top 10 words
    top_words = df_final_result_per_user.nlargest(n=10)
    top_10_words = list(top_words.index)
    return(df_tfidf_words[top_10_words])


for each_item in top_10_users: # Iterating through top 10 users
    owneruserid = str(each_item)
    selectTitle=df3.loc[df3['owneruserid'] == each_item]
    selectTitle.insert(0,'Owneruserid',each_item)
    tfidf=calculate_tfidf(selectTitle)
    tfidf.insert(0,'owneruserid',owneruserid)
    display(tfidf)

