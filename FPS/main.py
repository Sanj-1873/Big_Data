#!/usr/bin/python

import socket
import sys
import requests
import json
import time
import csv
import cryptography

from cryptography.fernet import Fernet
from pyspark.sql.context import SQLContext
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import lit


# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 5)




sqlContext = SQLContext(sc)
players_filepath = '/project/input/players.csv'
df_players = sqlContext.read.load(players_filepath, format='com.databricks.spark.csv', header='true',inferSchema='true')
df_players = df_players.withColumn('PlayerContribution', lit(0))
df_players = df_players.withColumn('PlayerPerformance', df_players.PlayerContribution)
df_players = df_players.withColumn('PlayerRanking', lit(0.5))
df_players = df_players.withColumn('Chemisty', lit(0.5))
df_players.show()
df_players.printSchema()



teams_filepath = '/project/input/teams.csv'
df_teams = sqlContext.read.load(teams_filepath, format='com.databricks.spark.csv', header='true',inferSchema='true')
df_teams.show()

#Set the checkpoint
ssc.checkpoint('/project/checkpoint')


dStream = ssc.socketTextStream("localhost", 6100)


#Pre-processing data

#Get workable json
get_json = dStream.map(lambda x: json.loads(x))


#Separate into event data
events_data = get_json.filter(lambda x: 'eventId' in list(x.keys()))
match_data = get_json.filter(lambda x:'competitionId' in list(x.keys()))

#Task 1

def pass_accuracy(event_data):
  #Task1 - Pass Accuracy
  #Select the correct Id
  passA = events_data.filter(lambda x: x['eventId']==8)
  #passA.pprint()
  #num 
  #num of accurate pass
  accurate_pass = passA.filter(lambda x: {"id":1801} in x['tags'])
  count_accurate_pass = accurate_pass.count()
  comp_accurate_pass = count_accurate_pass.map(lambda x: float(x))
  #count_accurate_pass.pprint()
  #num of accurate key pass
  accurate_key_pass = accurate_pass.filter(lambda x: {"id":302} in x['tags'])
  count_accurate_key_pass= accurate_key_pass.count()
  #count_accurate_key_pass.pprint()
  comp_accurate_key_pass = count_accurate_key_pass.map(lambda x: float(x) * 2)
  nume = comp_accurate_pass.union(comp_accurate_key_pass)
  numme = nume.reduce(lambda x,y : x+y )
  #Denom
  non_accurate_pass = passA.filter(lambda x: {"id":1802} in x['tags'])
  count_non_normal_pass = non_accurate_pass.count()
  count_normal_pass = passA.count()
  union_normal_pass = count_non_normal_pass.union(count_normal_pass)
  normal_pass_count = count_normal_pass.reduce(lambda x,y : x+y )
  comp_normal_pass = normal_pass_count.map(lambda x: float(x))
  #comp_normal_pass.pprint()
  key_passes = passA.filter(lambda x: {"id":702} in x['tags'])
  count_key_passes = key_passes.count()
  comp_key_passes = count_key_passes.map(lambda x: float(x) * 2)
  #comp_key_passes.pprint()
  denom_union =  comp_normal_pass.union(comp_key_passes)
  denom = denom_union.reduce(lambda x,y : x+y )
  #ans
  ans_union = numme.union(denom)
  ans = ans_union.reduce(lambda x,y : x/y )
  return ans 


#Task 2 - Duel Effectiveness

def duel_effectiveness(event_data):
  duelE = events_data.filter(lambda x: x['eventId']==1)
  countT = duelE.count()
  compT = countT.map(lambda x:float(x))
  #Process Neutral Duels 
  duelN = duelE.filter(lambda x: {"id":702} in x['tags'])
  countN = duelN.count()
  compN = countN.map(lambda x: float(x) * 0.5)
  #Process Duels Won
  duelW = duelE.filter(lambda x: {"id":703} in x['tags'])
  countlW = duelW.count()
  compW = countlW.map(lambda x:float(x))
  #Calculate wins + neutral 
  combine = compW.union(countN)
  sumWN = combine.reduce(lambda x,y : x+y )
  #Calculate ratio
  combineD =  sumWN.union(compT)
  ans = combineD.reduce(lambda x,y : x/y )
  ans.pprint()
  return ans

#Task 3 - free kick effectiveness
def free_kick(event_data):
  free_kick = events_data.filter(lambda x: x['eventId']==3)
  total_free_kicks = free_kick.count()
  comp_total_free_kicks = total_free_kicks.map(lambda x:float(x))

  #Assuming accurate pass to be effective free kick
  accurate_free_kick = free_kick.filter(lambda x: {"id":1801} in x['tags'])
  count_accurate_free_kick  = accurate_free_kick.count()
  comp_effective_free_kick = count_accurate_free_kick.map(lambda x:float(x))

  ##Number of penalties 
  penalties = free_kick.filter(lambda x: x['subEventId']==35)
  count_penalties = penalties.count()
  comp_penalties = count_penalties.map(lambda x:float(x))

  ##Cal numerator
  numm = comp_effective_free_kick.union(comp_penalties)
  nume = numm.reduce(lambda x,y : x+y )

  ##cal ans
  ansss = nume.union(comp_total_free_kicks)
  ans = ansss.reduce(lambda x,y : x/y )
  return ans
   


#Task 4
def shots(event_stream):
  shot = events_data.filter(lambda x: x['eventId']==10)
  total_shots = shot.count()
  comp_total_shots = total_shots.map(lambda x:float(x))

  #Shots on target
  on_target = shot.filter(lambda x: {"id":1801} in x['tags'])
  #shots on goals
  #on_goals = 
  #shots on target and goals 
  on_target_and_goals = on_target.filter(lambda x: {"id":101} in x['tags'])
  count_on_target_and_goals = on_target_and_goals.count()
  comp_count_on_target_and_goals = count_on_target_and_goals.map(lambda x:float(x))


  #ask someone how to fin shots on target but not goals 
  on_target_and_not_goals = on_target.filter(lambda x: {"id":101} not in x['tags'])
  count_shots_on_target_not_goals = on_target_and_not_goals.count()
  comp_count_shots_on_target_not_goals = count_shots_on_target_not_goals.map(lambda x:float(x))

  nume =  comp_count_on_target_and_goals.union(comp_count_shots_on_target_not_goals)
  num = nume.reduce(lambda x,y : x+y)

  ansu = num.union(total_shots)  
  ans = ansu.reduce(lambda x,y : x/y)
  return ans


  #Task 5
  def foul_loss(events_data):
      x = events_data.filter(lambda x: x['eventId']==2)
      ans = x.count()
      return ans

#Task 5 
def foul_loss(events_data):
    x = events_data.filter(lambda x: x['eventId']==2)
    ans = x.count()
    return ans


#Task 6
def own_goal(events_data):
    x = events_data.filter(lambda x: {"id":102} in x['tags'])
    ans = x.count()
    return ans

test = pass_accuracy(events_data)
# test.pprint()

#Functions to build the player profile
def player_contribution(playerId,events_data=events_data):
  
  players_data = events_data.filter(lambda x: x['playerId'] == playerId )
  pc_a = pass_accuracy(players_data)
  pc_fke = free_kick(players_data)
  pc_targes = shots(players_data)
  nu = pc_a.union(pc_fke)
  nr = nu.reduce(lambda x,y : x+y)
  nume = nr.union(pc_targes)
  a_num = nume.reduce(lambda x,y : x+y)

  ans = a_num.map(lambda x : x/4)
  return ans

def player_performance(playerId, events_data):
  #Find the num of fouls 
  players_data = events_data.filter(lambda x: x['playerId'] == playerId )
  fouls = foul_loss(players_data)
  fouls_comp = fouls.map(lambda x: (x*0.5)/100)
  player = player_contribution(playerId, events_data)

  own_goal = own_goal(players_data)
  comp_own_goal = own_goal.map(lambda x: (x*5)/100)

  tcu = fouls_comp.union(comp_own_goal)
  total_contribution = tcu.reduce(lambda x,y : 1-(x+y))
  ans_u = player.union(total_contribution)
  ans = ans_u.reduce(lambda x,y : x*y)
  
  return ans

def player_rating(playerId, events_data, existing_data):
  players_data = events_data.filter(lambda x: x['playerId'] == playerId )
  p_performace = player_performance(playerId, events_data)
  existing_player_rating =  existing_data.filter(lambda x: x['playerId'] == playerId )
 
  ans_u = p_performace.union(existing_player_rating)
  ans =  ans_u.reduce(lambda x,y: (x+y)/2)
  return ans

def chemistry(playerId1, playerId2, events_data, existing_data): 
  player_data1 = events_data.filter(lambda x: x['playerId'] == playerId1 )
  player_data2= events_data.filter(lambda x: x['playerId'] == playerId2 )

  player_rating1 = player_rating(playerId1, events_data, existing_data)
  player_rating2 = player_rating(playerId2, events_data, existing_data)

  
##Selecting the player ID from the events stream
#events_data.pprint()
player_id = events_data.map(lambda x: (x['playerId'],1))
#player_id.pprint()
player_id_window = player_id.reduceByKeyAndWindow((lambda x,y: x+y), 5, 5)
#player_id_window.pprint()
player_idss = player_id_window.map(lambda x:x[0])
testad = player_contribution(0)
testad.pprint()

# player_contri = player_idss.transform(lambda x:)
# player_contri.pprint()


ids = df_players.select("Id")
ids.collect()




ssc.start()             # Start the computation
ssc.awaitTermination()   # Wait for the computation to terminate