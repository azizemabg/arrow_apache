###############################################################################
#
#
#       WORKING WITH ARROW APACHE ON R

library(usethis)

# download a copy of this repository
usethis::create_from_github(
  repo_spec = "bixcop18/module_3_intro_NGS", 
  destdir = "."
)

usethis::create_from_github(
  repo_spec = "bixcop18/hpc_and_cloud_computing_pipelines", 
  destdir = "."
)


  psoerensen/qgg

# install the package dependencies
remotes::install_deps()

# manually download and unzip the "tiny taxi" data
download.file(
  url = "https://github.com/djnavarro/arrow-user2022/releases/download/v0.1/nyc-taxi-tiny.zip",
  destfile = "nyc-taxi-tiny.zip"
)
unzip(
  zipfile = "nyc-taxi-tiny.zip", 
  exdir = "data"
)

# Packages and Data #####

install.packages(c(
  "arrow", "dplyr", "dbplyr", "duckdb", "fs", "janitor",
  "palmerpenguins", "remotes", "scales", "stringr", 
  "lubridate", "tictoc"
))
install.packages(c("ggplot2", "ggrepel", "sf"))

library(arrow)
library(dplyr)
library(dbplyr)
library(duckdb)
library(stringr)
library(lubridate)
library(palmerpenguins)
library(tictoc)
library(scales)
library(janitor)
library(fs)
library(ggplot2)
library(ggrepel)
library(sf)
library(data.table)
library(tidyverse)
library(skimr)


n_cores <- parallel::detectCores()
n_cores  
setDTthreads(n_cores)

getwd()

load("ams.data.RData")
class(ams.data)
dim(ams.data)

ams.data.subset <- ams.data[1:300000, ]
head(ams.data.subset)

ams.data.arrow <- arrow::as_arrow_table(ams.data.subset)
class(ams.data.arrow)
head(ams.data.arrow, 5)
glimpse(ams.data.arrow)

ams.data.arrow <- ams.data.arrow %>% 
  mutate(date_milking = as.Date(milking_date)) %>% 
  collect()
class(ams.data.arrow)
ams.data.arrow <- as_arrow_table(ams.data.arrow)
names(ams.data.arrow)
skim(ams.data.arrow)

ams.data.arrow %>% 
  group_by(animalID, date_milking, breed, parity, herdID, dob, doc, origin) %>% 
  summarise(n_visit = n(), 
            tot_milk_kg = sum(milkyield_kg), 
            tot_duration = sum(milkduration), 
            tot_interval = sum(milkinginterval), 
            avg_milk_kg = mean(milkyield_kg), 
            avg_duration = mean(milkduration), 
            avg_interval = mean(milkinginterval), 
            age_year = round(as.numeric(date_milking - dob))/31536000, 
            DIM = round(as.numeric((date_milking - doc)/24))) %>% 
  collect()

ams.data.animal <- ams.data.arrow %>% 
  group_by(animalID, date_milking, breed, parity, herdID, dob, doc, origin) %>% 
  summarise(n_visit = n(), 
            tot_milk_kg = sum(milkyield_kg), 
            tot_duration = sum(milkduration), 
            tot_interval = sum(milkinginterval), 
            avg_milk_kg = mean(milkyield_kg), 
            avg_duration = mean(milkduration), 
            avg_interval = mean(milkinginterval)) %>% 
  collect()

ams.data.animal <- as_arrow_table(ams.data.animal)
class(ams.data.animal)

names(ams.data.animal)
ams.data.animal <- ams.data.animal %>% 
  mutate(age_year = round(as.numeric(date_milking - dob))/31536000, 
             DIM = round(as.numeric((date_milking - doc)/24))) %>% 
  collect()

ams.data.animal <- ams.data.animal %>% 
  mutate(date_milking = lubridate::with_tz(date_milking, "Europe/Vienna"),
         dob = lubridate::with_tz(dob, "Europe/Vienna"),
         doc = lubridate::with_tz(doc, "Europe/Vienna"),
         age_year = as.numeric(round(as.duration(date_milking - dob)/dyears(1), 2)), 
         DIM = as.numeric(round(as.duration(date_milking - doc)/ddays(1), 0))) %>% 
  collect()

ams.data.305 <- ams.data.animal %>% 
  filter(DIM %in% 11:305) %>% 
  collect()
range(ams.data.305$DIM)
ams.data.305 %>% 
  group_by(animalID, DIM) %>% 
  summarise(n_records = n()) %>% 
  collect()

an.1 <- ams.data.305 %>% 
  filter(animalID == "00144a3300b38809dab1d835e4e625a5", year == 2019) %>%
  arrange(DIM) %>% 
  collect()

ams.data.305 <- ams.data.305 %>% 
  mutate(year = year(date_milking), 
         month = month(date_milking), 
         day = day(date_milking)) %>% 
  collect()

ams.data.305 %>% 
  group_by(animalID) %>% 
  summarise(n = n()) %>% 
  collect()

###################################################################################################
#
#
# ARROW TUTORIAL FROM R PACKAGE 

library(tidyverse)
library(datasets)
library(tictoc)
library(microbenchmark)
library(arrow)
library(skimr)

library(arrow, warn.conflicts = FALSE)

# creating arrow table
dat <- arrow_table(x = 1:3, y = c("a", "b", "c"))
dat
head(dat, 2)

# subset arrow table
dat[1:2, 1:2]

# using dollar symbol
dat$x
dat$y # individual column in arrow table are represented as chunked arrays that is one dimensional array
# Datasets which are used for data stored on-disk rather than in-memory

getwd()

file_path <- tempfile(fileext = ".parquet")
write_parquet(starwars, file_path)
write_parquet(starwars, "starwars.parquet")

sw_table <- read_parquet("starwars.parquet", as_data_frame = FALSE)
print(sw_table)
class(sw_table)

set.seed(1234)
nrows <- 100000
random_data <- data.frame(
  x = rnorm(nrows),
  y = rnorm(nrows),
  subset = sample(10, nrows, replace = TRUE)
)
random_data

random_data %>%
  group_by(subset) %>%
  write_dataset("random_data")

list.files("random_data", recursive = TRUE)

rd <- open_dataset("random_data")

names(random_data)
class(random_data)

names(rd)
class(rd)

rd %>% 
  group_by(x, y) %>% 
  summarise(avg_n = mean(n), 
            avg_y = mean(y)) %>% 
  collect()

write_dataset("random_nosubset")
write_dataset(dataset = random_data, path = "random_nonsubset")

rd %>%
  group_by(subset) %>%
  summarize(mean_x = mean(x), min_y = min(y)) %>%
  filter(mean_x > 0) %>%
  arrange(subset) %>%
  collect()

load("ams.data.RData")
class(ams.data)

ams.data <- write_dataset(dataset = ams.data, path = "ams.data")
ams.data <- open_dataset("ams.data")
class(ams.data)

system.time(ams.data %>% 
  group_by(animalID) %>% 
  summarise(n_records = n()) %>% 
  collect())

ams.data <- ams.data %>% 
  mutate(date_milking = as.Date(milking_date)) %>% 
  collect()

names(ams.data)

ams.data <- write_dataset(dataset = ams.data, path = "ams.data.fix")
ams.data.fix <- open_dataset("ams.data.fix")
class(ams.data.fix)

ams.data.animal <- ams.data.fix %>%
  group_by(animalID, date_milking, breed, parity, herdID, dob, doc, origin) %>% 
  summarise(n_visit = n(), 
            tot_milk_kg = sum(milkyield_kg), 
            tot_duration = sum(milkduration), 
            tot_interval = sum(milkinginterval), 
            avg_milk_kg = mean(milkyield_kg), 
            avg_duration = mean(milkduration), 
            avg_interval = mean(milkinginterval)) %>% 
  collect()
names(ams.data.fix)
head(ams.data.animal, 5)
class(ams.data.animal)

names(ams.data.animal)
class(ams.data.animal$dob)
class(ams.data.animal$doc)
class(ams.data.animal$date_milking)

ams.data.animal <- write_dataset(dataset = ams.data.animal, path = "ams.data.animal")
ams.data.animal <- write_dataset(dataset = ams.data.animal, path = "ams.data.animal", max_partitions = 40000)

ams.data.animal <- as_arrow_table(ams.data.animal)
class(ams.data.animal)

ams.data.animal <- write_dataset(dataset = ams.data.animal, path = "ams.data.animal")
ams.data.animal <- open_dataset("ams.data.animal")
class(ams.data.animal)

ams.data.animal <- ams.data.animal %>% 
  mutate(date_milking = lubridate::with_tz(date_milking, "Europe/Vienna"),
          dob = lubridate::with_tz(dob, "Europe/Vienna"),
          doc = lubridate::with_tz(doc, "Europe/Vienna"),
          age_year = as.numeric(round(as.duration(date_milking - dob)/dyears(1), 2)), 
          DIM = as.numeric(round(as.duration(date_milking - doc)/ddays(1), 0))) %>%
  collect()

ams.data.animal <- ams.data.animal %>% 
  mutate(date_milking = lubridate::with_tz(date_milking, "Europe/Vienna"),
         dob = lubridate::with_tz(dob, "Europe/Vienna"),
         doc = lubridate::with_tz(doc, "Europe/Vienna")) %>% 
  collect() %>%
  mutate(age_year = as.numeric(round(as.duration(date_milking - dob)/dyears(1), 2)), 
         DIM = as.numeric(round(as.duration(date_milking - doc)/ddays(1), 0)))
head(ams.data.animal, 5)
class(ams.data.animal)

ams.data.animal <- write_dataset(dataset = ams.data.animal, path = "ams.data.animal", max_partitions = 40000)
ams.data.animal <- open_dataset("ams.data.animal")
class(ams.data.animal)
head(ams.data.animal, 5)

ams.data.305 <- ams.data.animal %>% 
  filter(DIM %in% 11:305) %>% 
  collect()
class(ams.data.305)
head(ams.data.305, 5)
range(ams.data.305$DIM)


ams.data.305 <- write_dataset(dataset = ams.data.305, path = "ams.data.305", max_partitions = 40000)
ams.data.305 <- open_dataset("ams.data.305")

ams.data.305 %>% 
  filter(animalID == "c096bbe9e06604203f653bbd9bc9a7f3") %>% 
  collect() 

ams.data.305 <- ams.data.305 %>% 
  mutate(year = year(date_milking), 
         month = month(date_milking), 
         day = day(date_milking)) %>% 
  relocate(animalID, age_year, DIM, date_milking, year, month, day, breed, parity, 
           herdID, dob, doc, origin, n_visit, tot_milk_kg, tot_duration,
           tot_interval, avg_milk_kg, avg_duration, avg_interval) %>% 
  collect()

breed <- ams.data.305 %>%
  select(breed) %>% 
  collect()
class(breed$breed)
breed$breed <- as.character(breed$breed)
breed <- unique(breed)


ams.data.305 <- write_dataset(dataset = ams.data.305, path = "ams.data.305")
ams.data.305 <- open_dataset("ams.data.305")
class(ams.data.305)
vis_miss(ams.data.305)

ams.data.breed <- write_dataset(dataset = ams.data.305, path = "ams.data.305", partitioning = "breed")
breed
ams.data.breed <- open_dataset("ams.data.305")
class(ams.data.breed)
vis_miss(ams.data.breed)
skim(ams.data.305)


ams.data.breed %>% 
  group_by(year) %>% 
  filter(animalID == "10482390ea377c4a1a2475763b12b38d") %>% 
  collect()

ams.data.305 %>% 
  group_by(animalID) %>% 
  summarise(avg_age = mean(tot_milk_kg)) %>% 
  collect()

ams.data.fix <- open_dataset("ams.data.fix")
class(ams.data.fix)

install.packages("Hmisc")
library(Hmisc)
describe(ams.data.fix)
install.packages("DataExplorer")
library(DataExplorer)
DataExplorer::create_report(ams.data.fix)
create_report(diamonds)
class(diamonds)
DataExplorer::plot_intro(diamonds)
plot_bar(diamonds)
class(ams.data.fix)

diamonds <- write_dataset(diamonds, "diamonds", partitioning = c("cut", "color"))
diamonds <- open_dataset("diamonds")
class(diamonds)
diamonds
describe(diamonds)
skim(diamonds)
summary(diamonds)
skimr::base_skimmers(diamonds)
skimr::complete_rate(diamonds)
skimr::get_skimmers(diamonds$carat)
skimr::skim_tee(diamonds, skim_fun = skim)
skimr::n_unique(diamonds)
skim(diamonds, .data_name = carat)

ams.data.fix %>% 
  data.table::as.data.table() %>% 
  head(5) %>% 
  collect()

ams.data.fix[, c(8:13), with = FALSE]
ams.data.fix[breed = "HF"]
system.time(ams.data.fix %>% 
  group_by(breed) %>% 
  filter(breed == "HF") %>%
  collect())
names(ams.data.fix)

system.time(ams.data.fix %>% 
  filter(breed == "HF") %>% 
  collect())

ams.data.fix.table <- data.table::as.data.table(ams.data.fix)
dim(ams.data.fix)
dim(ams.data.fix.table)
class(ams.data.fix.table)

head(ams.data.fix.table, 5)

system.time(ams.data.fix.table[breed == "HF" & date_milking == 2019-03-20])
ams.data.fix.table[breed == "HF" & date_milking == 2019-03-20]

ams.data.animal <- open_dataset("ams.data.animal")
ams.data.animal

names(ams.data.animal)

ams.data.animal <- ams.data.animal %>% 
  mutate(year = year(date_milking), 
         month = month(date_milking), 
         day = day(date_milking)) %>% 
  collect()
names(ams.data.animal)

ams.data.animal <- write_dataset(ams.data.animal, path = "ams.data.animal", partitioning = c("breed", "year", "parity"))
ams.data.animal <- open_dataset("ams.data.animal")
names(ams.data.animal)
ams.data.animal <- ams.data.animal %>% 
  relocate(animalID, date_milking, year, month, day, DIM, age_year, breed, herdID, dob, doc, origin, 
           n_visit, tot_milk_kg, tot_duration, tot_interval, avg_milk_kg, avg_duration, avg_interval) %>% 
  collect()

library(data.table)
names(ams.data.animal)
ams.data.animal <- setcolorder(as.data.table(ams.data.animal), 
                               c("animalID", "date_milking", "year", "month", "day", "DIM", 
                                 "age_year", "breed", "herdID", "dob", "doc", "origin", 
                                 "n_visit", "tot_milk_kg", "tot_duration", "tot_interval", 
                                 "avg_milk_kg", "avg_duration", "avg_interval"))

ams.data.animal %>% 
  filter(DIM %in% 11:305 & breed %in% c("HF", "FL", "RF", "BS")) %>% 
  collect()

ams.data.animal %>% 
  filter(breed %in% c("HF", "FL", "RF", "BS")) %>% 
  collect()
names(ams.data.animal)

ams.subset <- ams.data.animal[1:1000000, ]
names(ams.subset)
ams.subset.arrow <- data.table::as.data.table(ams.subset)



# data.table 
head(ams.subset.arrow, 5)
system.time(ams.subset.arrow[breed == "AA"])
system.time(ams.subset.arrow[parity > 5 & tot_milk_kg >= 10])
system.time(ams.subset.arrow[, .(milk_breed = mean(tot_milk_kg), dur = mean(tot_duration), int = mean(tot_interval)), by = breed])
microbenchmark::microbenchmark(ams.subset.arrow[, .(milk_breed = mean(tot_milk_kg), dur = mean(tot_duration), int = mean(tot_interval)), by = breed])
head(ams.subset.arrow, 5)

ams.subset.arrow %>% 
  filter(DIM %in% c(11:305) & breed %in% c("HF", "FL", "RF", "BS")) %>% 
  group_by(breed, animalID, DIM) %>% 
  summarise(milk = mean(tot_milk_kg)) %>% 
  filter(breed == "BS") %>% 
  ggplot(aes(x = DIM, y = milk)) +
  geom_line() +
  geom_smooth() +
  theme_bw()

ams.subset.arrow %>% 
  filter(DIM %in% c(11:305) & breed %in% c("HF", "FL", "RF", "BS")) %>% 
  group_by(breed, animalID, DIM) %>% 
  summarise(milk = mean(tot_milk_kg)) %>% 
  filter(animalID == "0003b49969eaf74c3271ac473e43d8ae") %>% 
  ggplot(aes(x = DIM, y = milk)) +
  geom_line() +
  geom_smooth() +
  theme_bw()

fst::threads_fst(12)
fst::threads_fst()

# arrow
system.time(ams.subset %>% 
              filter(breed == "AA") %>% 
              collect())
system.time(ams.subset %>% 
              filter(parity > 5 & tot_milk_kg >= 10) %>% 
              collect())
system.time(ams.subset %>% 
  group_by(breed) %>% 
  summarise(milk_breed = mean(tot_milk_kg), dur = mean(tot_duration), int = mean(tot_interval)) %>% 
  collect())
microbenchmark::microbenchmark(ams.subset %>% 
                                 group_by(breed) %>% 
                                 summarise(milk_breed = mean(tot_milk_kg), dur = mean(tot_duration), int = mean(tot_interval)) %>% 
                                 collect())

num_cores <- detectCores()
setDTthreads(num_cores)
getDTthreads()
