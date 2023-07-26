################################################################################
################################################################################
#
#
#                     DATA.TABLE
#
#
#
#
#
################################################################################

library(data.table)
library(tidyverse)

# set up the threads
num_thread <- data.table::getDTthreads()
setDTthreads(num_thread)

# load the datasets
load("ams.animal.test.RData")
range(ams.animal.test$milk_date)

# adding DIM column
ams.animal.test[, DIM := round(as.numeric((milk_date - doc)/24))]
ams.animal.test$DIM <- NULL

# checking class variable
lapply(ams.animal.test, function(x) class(x))
range(ams.animal.test$totmilk_kg)
range(ams.animal.test$DIM)
ams.animal.test[DIM == 1488, ][, doc] - ams.animal.test[DIM == 1488, ][, milk_date]

# subsetting dim range from 4 till 360
dim.oke <- ams.animal.test[DIM >= 1 & DIM <= 360]
range(dim.oke$DIM)

# creating two variables dim and milk yield (as average)
dim.milk <- ams.animal.test %>% 
  group_by(DIM) %>% 
  summarise(milk_yield = mean(totmilk_kg))

# Visualizing the trends or fluctuations from dim 5 till 360
dim.milk %>% 
  filter(DIM >= 5 & DIM <= 360) %>% 
  ggplot(aes(x = DIM, y = milk_yield)) +
  geom_line() +
  xlim(0, 360) +
  theme_classic()
  
dim.milk %>% 
  filter(DIM >= 5 & DIM <= 360) %>% 
  ggplot(aes(x = DIM, y = milk_yield)) +
  geom_line() +
  geom_smooth(method = "loess", se = TRUE) +
  theme_classic() +
  scale_x_continuous(breaks = c(0, 20, 40, 60, 80, 100, 120, 140, 160, 180, 200, 220, 240, 260, 280, 300, 320, 340, 360)) 

dim.milk %>% 
  filter(DIM >= 5 & DIM <= 360) %>% 
  ggplot(aes(x = DIM, y = milk_yield)) +
  geom_line() +
  geom_smooth(method = "loess", se = TRUE, level = 0.99, alpha = 0.2) +
  theme_classic() +
  scale_x_continuous(breaks = c(0, 20, 50, 80, 100, 120, 150, 180, 200, 220, 250, 280, 300, 320, 340, 360))
  
  
  
  
save(list = ls(all.names = TRUE), file = "data.table.RData")
#############################################################################
# Addding SD line

library(dplyr)

