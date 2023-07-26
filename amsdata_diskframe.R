load("ams.data.RData")

library(tidyverse)
library(data.table)
library(future)
library(skimr)
library(arrow)
library(disk.frame)


setup_disk.frame(workers = data.table::getDTthreads(), 
                 future_backend = future::multisession)
options(future.globals.maxSize = Inf)

# Converting to disk.frame data
ams.data.df <- as.disk.frame(ams.data, outdir = "ams.data.df", 
                             overwrite = TRUE) # store it in current working directory

# Checking before and after conversion
system.time(head(ams.data, 2))
head(ams.data.df, 2)
identical(head(ams.data, 2), head(ams.data.df, 2)) # Same as before! Carry on
class(ams.data.df)

# dplyr
ams.data.df <- ams.data.df %>% 
  mutate(date_milking = as.Date(milking_date)) %>% 
  collect()


class(ams.data.df)
ams.data.df <- as.disk.frame(ams.data.df, outdir = "ams.data.df", 
                             overwrite = TRUE)
head(ams.data.df, 2)
head(ams.data, 2)


# using disk.frame
ams.data.animal <- ams.data.df %>% 
  group_by(animalID, date_milking, herdID, origin, dob, breed, parity, doc, next_doc) %>% 
  summarise(n_visit = length(milking_date),
            tot_milk_kg = sum(milkyield_kg), 
            tot_duration_min = sum(milkduration), 
            tot_interval_min = sum(milkinginterval),
            avg_milk_kg = mean(milkyield_kg), 
            avg_duration_min = mean(milkduration), 
            avg_interval_min = mean(milkinginterval)) %>% 
  collect()

rm(list = ls())
