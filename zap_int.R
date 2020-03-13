
################################
###         PART ONE         ###
################################

install.packages("RJDBC")
install.packages("RPostgreSQL")
install.packages("data.table")
install.packages("scales")
library(RJDBC)
library(RPostgreSQL)
library(data.table)
library(scales)

drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv,dbname='zapier',host='public-redshift.zapier.com',port =5439,user='hrivera',password='hrivera1234A' )


base_table <- dbGetQuery(con,'select * from source_data.tasks_used_da ')

length(unique(base_table$user_id))
length(unique(base_table$account_id))
summary(base_table)
summary(base_table$sum_tasks_used)

tasks_by_user <- aggregate(sum_tasks_used~user_id, base_table, sum)
users_per_account <- aggregate(user_id~account_id, base_table, function(x) length(unique(x)))

base_table <- dbGetQuery(con,'create table base_table as select * from source_data.tasks_used_da where sum_tasks_used > 0 ')
base_table <- dbGetQuery(con,'select * from hrivera.base_table where sum_tasks_used > 0 ')

################################
###         PART TWO         ###
################################
### CREATE CALENDAR TABLE
### LOOK FOR ALL THE DATES WITHIN THE TIMEFRAME TO FILL IN FOR USER GAPS LATER
dbGetQuery(con,"create table calendar as select distinct date from source_data.tasks_used_da ")
calendar <- dbGetQuery(con,"select * from hrivera.calendar")

### ALL INTERVALS FOR EACH USER

intervals <- dbGetQuery(con,"CREATE TABLE all_intervals AS (
                    WITH events AS (
                        SELECT user_id
                        ,date AS event_dt
                        ,1 AS days_active_delta
                        FROM hrivera.base_table
                        UNION ALL
                        SELECT user_id
                        ,date + 28 AS event_dt
                        ,-1 AS days_active_delta
                        FROM hrivera.base_table),

                        rolling_states AS (
                        SELECT user_id
                        ,event_dt
                        ,SUM(days_active_delta) OVER (
                        PARTITION BY user_id
                        ORDER BY event_dt ASC
                        ROWS UNBOUNDED PRECEDING
                        ) > 0 AS active
                        FROM (
                        SELECT user_id
                        ,event_dt
                        ,SUM(days_active_delta) AS days_active_delta
                        FROM events
                        GROUP BY 1, 2
                        )
                        ),

                        deduplicated_states AS (
                        SELECT user_id
                        ,event_dt
                        ,active
                        FROM (
                        SELECT user_id
                        ,event_dt
                        ,active
                        ,COALESCE(
                        active = LAG(active, 1) OVER (
                        PARTITION BY user_id
                        ORDER BY event_dt ASC
                        ),
                        FALSE
                        ) AS redundant
                        FROM rolling_states
                        )
                        WHERE NOT redundant
                        )
                        SELECT user_id
                        ,start_dt
                        ,end_dt
                        FROM (
                        SELECT user_id
                        ,event_dt AS start_dt
                        ,COALESCE(
                        LEAD(event_dt, 1) OVER (
                        PARTITION BY user_id
                        ORDER BY event_dt ASC
                        ),
                        CURRENT_DATE
                        ) - 1 AS end_dt
                        ,active
                        FROM deduplicated_states
                        )
                        WHERE active
                        )")

all_intervals <- dbGetQuery(con,"select * from hrivera.all_intervals")

#Active User Counts by Day
users_by_day <- dbGetQuery(con,"select date, count(user_id) 
                           from calendar,all_intervals
                              where start_dt = date or 
                                    (date > start_dt and date <=end_dt)
                              group by date")

#Get a user per day look at actives
list_users_by_day <- dbGetQuery(con,"select date, user_id 
                           from calendar,all_intervals
                           where start_dt = date or 
                           (date > start_dt and date <=end_dt)")


#Churned Users by Day
churns_by_day <- dbGetQuery(con,"select date, count(user_id) 
                           from calendar,all_intervals
                              where (start_dt != date and date > end_dt) and
                              (date-29 < end_dt)
                              group by date")

#Get a user per day look at churns
list_churns_by_day <- dbGetQuery(con,"select date, user_id 
                           from calendar,all_intervals
                            where (start_dt != date and date > end_dt) and
                            (date-29 < end_dt)")


################################
###       PART THREE         ###
################################

user_list <- unique(list_users_by_day$user_id)
churn_list <- unique(list_churns_by_day$user_id)

###never churn 
never_churn <- setdiff(user_list,churn_list)

users_never_churn <- base_table[base_table$user_id %in% never_churn,]
summary(users_never_churn$sum_tasks_used)
users_never_churn <- users_never_churn[order(users_never_churn$user_id,users_never_churn$date),]
users_never_churn <- setDT(users_never_churn)[ ,time_btn := date - lag(date), by=user_id]
users_never_churn <- setDT(users_never_churn)[ , mean_time_btn := mean(time_btn,na.rm = T) , by=user_id]
mean(unique(users_never_churn$mean_time_btn),na.rm=T)

#Churned and then came back
user_churn_came_back <- aggregate(start_dt ~user_id,all_intervals,length)
user_churn_came_back2 <- user_churn_came_back[!user_churn_came_back$user_id %in% users_never_churn$user_id,]
user_churn_came_back3 <- subset(user_churn_came_back2, start_dt > 1)
user_churn_came_back4 <- base_table[base_table$user_id %in% user_churn_came_back3$user_id,]
summary(user_churn_came_back4$sum_tasks_used)
user_churn_came_back4 <- user_churn_came_back4[order(user_churn_came_back4$user_id,user_churn_came_back4$date),]
user_churn_came_back4 <- setDT(user_churn_came_back4)[ ,time_btn := date - lag(date), by=user_id]
user_churn_came_back4 <- setDT(user_churn_came_back4)[ , mean_time_btn := mean(time_btn[which(user_churn_came_back4$time_btn <29)],na.rm = T) , by=user_id]
mean(unique(user_churn_came_back4$mean_time_btn),na.rm=T)

#active once churned and never came back 
churn_never_returned <- subset(user_churn_came_back2, start_dt == 1)
churn_never_returned2 <- base_table[base_table$user_id %in% churn_never_returned$user_id,]
summary(churn_never_returned2$sum_tasks_used)
churn_never_returned2 <- churn_never_returned2[order(churn_never_returned2$user_id,churn_never_returned2$date),]
churn_never_returned2 <- setDT(churn_never_returned2)[ ,time_btn := date - lag(date), by=user_id]
churn_never_returned2 <- setDT(churn_never_returned2)[ , mean_time_btn := mean(time_btn,na.rm = T) , by=user_id]
mean(unique(churn_never_returned2$mean_time_btn),na.rm=T)

users_by_day$active <- 1
#ggplot(users_by_day,aes(date, count), group=1) + geom_line() + ggtitle("Active Users Over Time")

churns_by_day$active <- 0
#ggplot(churns_by_day,aes(date, count), group=1) + geom_line() + ggtitle("Churns Users Over Time")

#### PLOTS
all_by_day <- rbind.data.frame(users_by_day,churns_by_day)
ggplot(all_by_day,aes(date, count,factor=active), color=active) + geom_line(aes(y=)) + ggtitle(" Users Over Time")
ggplot(all_by_day, aes(x = date, y = count,
                      group = active,
                      colour = factor(active))) + 
  geom_line(size=1) + 
  scale_color_discrete(name = 'User', labels = c("Churn", "Active"))+
  ggtitle("Number of Daily Active User v Churned Users")


###churn rate 
churn_rate <- merge(churns_by_day,users_by_day,by='date',all.y = T)
churn_rate$rate <- churn_rate$count.x/churn_rate$count.y
ggplot(churn_rate,aes(date,rate)) + geom_line() + ggtitle("Churn Rate Over Time")

### tasks by actives only
aonly <- rbind(user_churn_came_back4,users_never_churn)
aonly2 <- aggregate(sum_tasks_used~date,aonly,sum)
aonly2$active <- 1
#ggplot(aonly2,aes(date,sum_tasks_used)) +geom_line()

churn_never_returned3 <- aggregate(sum_tasks_used~date,churn_never_returned2,sum)
churn_never_returned3$active <- 0 

tasks_all <- rbind(aonly2,churn_never_returned3)

ggplot(tasks_all, aes(x = date, y = sum_tasks_used,
                       group = active,
                       colour = factor(active))) + 
  geom_line(size=1) + 
  scale_color_discrete(name = 'User', labels = c("Churn", "Active")) +
  scale_y_continuous(labels=scales::comma) +
  ggtitle("Number of Tasks per Active/Re-Engaged Users v. Active then Churned Users Over Time")
