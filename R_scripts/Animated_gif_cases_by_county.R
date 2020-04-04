library(tidyverse)
library(gganimate)
library(magick)
library(censusapi)
library(foreach)

#----------------------------------------------------------------------------------------------------------------------
# Function below takes in NY Times county data and several user-specified arguments
# Its purpose to is clean up and re-assign death and case counts within a given state/region based on population
# The user needs to pass the state of interest along with population, fips, and county names of interest
# The function will return a data.frame in which all cases and deaths assigned a missing fips code are re-assigned 
# Re-assignment is proportional to the population values for the targeted counties 
fips_na_helper <- function(df, state, pop_vals, fips_vals, county_names, missing_fips){
  #browser()
  tot_tmp <- sum(pop_vals)
  prop_pop <- pop_vals/tot_tmp
  
  df_tmp <- df[df$state == state, ]
  unique_dates <- unique(df_tmp$date)
  
  # Aggregates cases and deaths when fips codes are missing and redistributes within state based on population
  for(d in 1:length(unique_dates)){
    tot_case_nas <- sum(df_tmp$cases[df_tmp$date == unique_dates[d] & df_tmp$fips == missing_fips])
    tot_death_nas <- sum(df_tmp$deaths[df_tmp$date == unique_dates[d] & df_tmp$fips == missing_fips])
    
    if(sum(df_tmp$fips[df_tmp$date == unique_dates[d]] == missing_fips) > 0){
      print(paste("Re-assinging a total of", tot_case_nas, "cases on", unique_dates[d], "in", state))
      cases_redist <- round(tot_case_nas*prop_pop)
      
      print(paste("Re-assinging a total of", tot_death_nas, "deaths on", unique_dates[d], "in", state))
      deaths_redist <- round(tot_death_nas*prop_pop)
      
      if(sum(cases_redist) > tot_case_nas){
        case_diff <- sum(cases_redist) - tot_case_nas
        cases_redist[which.max(prop_pop)] <- cases_redist[which.max(prop_pop)] - case_diff
      }
      
      if(sum(cases_redist) < tot_case_nas){
        case_diff <- tot_case_nas - sum(cases_redist)
        cases_redist[which.max(prop_pop)] <- cases_redist[which.max(prop_pop)] + case_diff
      }
      
      if(sum(deaths_redist) > tot_death_nas){
        death_diff <- sum(deaths_redist) - tot_death_nas
        deaths_redist[which.max(prop_pop)] <- deaths_redist[which.max(prop_pop)] - death_diff
      }
      
      if(sum(deaths_redist) < tot_death_nas){
        death_diff <- tot_death_nas - sum(deaths_redist)
        deaths_redist[which.max(prop_pop)] <- deaths_redist[which.max(prop_pop)] + death_diff
      }
      
      # Bringing back in original case and death counts to be added to redistributed totals per fips
      df_tmp_w_fips <- df_tmp[df_tmp$date == unique_dates[d] & df_tmp$fips != missing_fips,]
      
      # Identifying instances that need to be "added" to: 
      df_redist <- data.frame(fips=fips_vals, 
                              date=rep(unique_dates[d]),
                              cases_redist=cases_redist, 
                              deaths_redist=deaths_redist, 
                              county_rename=county_names, stringsAsFactors = FALSE)
      
      df_comb <- merge(df_redist, df_tmp_w_fips, by=c("fips", "date"), all=TRUE)
      
      # Summing and recoding as needed. 
      df_comb$cases <- rowSums(df_comb[,c("cases", "cases_redist")], na.rm = TRUE)
      df_comb$deaths <- rowSums(df_comb[,c("deaths", "deaths_redist")], na.rm = TRUE)
      df_comb$county <- ifelse(is.na(df_comb$county), df_comb$county_rename, df_comb$county)
      df_comb$state <- state
      
      insert_df <- df_comb[,c("merge_id", "date", "county", "state", "fips", "cases", "deaths")]
      
      df <- df[!(df$state == state & df$date == unique_dates[d]), ]
      df <- rbind(df, insert_df)
      df <- df[order(df$date, df$state, df$fips),]
    }
  }
  return(df)
}

#----------------------------------------------------------------------------------------------------------------------
# Bringing in Census data regarding county population
# Main usage is going to be to re-distribute Unknown cases across a state's counties using populations of those counties
# Incorporating most recent data avaiable - 2019
CENSUS_API_KEY <- "964ccab1d7dd4a374d6be1fb807ea3a3af354a1e"
target_year <- 2019

county_population <- getCensus(name="pep/population", vintage = target_year, key = CENSUS_API_KEY, 
                               region = "county:*", vars = c("POP"))

county_population$POP <- as.numeric(county_population$POP) 
county_population$fips <- as.numeric(paste0(county_population$state, county_population$county))

state_population <- county_population %>% 
  select(state, POP) %>% 
  group_by(state) %>% 
  summarize(STATE_POP = sum(POP))

dat_population <- merge(county_population, state_population, by="state")
dat_population$CNTY_PROP_POP <- dat_population$POP/dat_population$STATE_POP

#----------------------------------------------------------------------------------------------------------------------
# Now bringing in NY Times data
dat <- read.csv("~/covid-19-data/us-counties.csv", 
                stringsAsFactors = FALSE)

# Removing estimates from states outside of those included in the map to be used
us_map <- usmap::us_map(regions="counties")
us_map$fips <- as.numeric(us_map$fips)

states_names <- unique(us_map$full)
dat <- dat[ifelse(dat$state %in% states_names, TRUE, FALSE), ]

# Addressing "Unknown" counts by redistributing them around the state using county population. 

# Creating a row for every valid fips on every valid date 
# Adding a missing value to fips codes
dat$fips[is.na(dat$fips) & dat$county == "Unknown"] <- -999
dat$fips[is.na(dat$fips) & dat$county != "Unknown"] <- -998

fips_state_df <- unique(us_map[,c("fips", "full")])

state_fip <- paste(fips_state_df$full, fips_state_df$fips, sep="_")
valid_dates <- unique(dat$date)

cl <- parallel::makeCluster(10)
doParallel::registerDoParallel(cl)

par_func <- function(var1, var2){
  foreach(i = 1:length(var1), .combine = "c") %dopar% {
    paste(var1[i], var2, sep="_")
  }
}

date_state_fips <- par_func(valid_dates, state_fip)

parallel::stopCluster(cl)

dat$merge_id <- paste(dat$date, dat$state, dat$fips, sep="_")

merge_df <- data.frame(merge_id = date_state_fips, stringsAsFactors = FALSE)
dat_all <- merge(merge_df, dat, by="merge_id", all = TRUE)

# Unpacking merge id variable and putting date and state back in...
unpacked_merge_id <- unlist(strsplit(dat_all$merge_id, split = "_"))
by_3s <- seq(0, length(unpacked_merge_id)-3, by=3)

dat_all$date <- unpacked_merge_id[1+by_3s]
dat_all$state <- unpacked_merge_id[(2+by_3s)]
dat_all$fips <- unpacked_merge_id[(3+by_3s)]

# Time to trim down the data.frame - removing the missing codes first that won't do anything 
dat_all$cases <- ifelse(is.na(dat_all$cases), 0, dat_all$cases)
dat_all$deaths <- ifelse(is.na(dat_all$deaths), 0, dat_all$deaths)

# Removing Unknown cases that have 0 cases and zero deaths to distribute around a state
dat_all <- dat_all[!(dat_all$fips == "-999" & dat_all$deaths == 0 & dat_all$cases == 0),]

# Removing other forms of missing fips codes that will have no bearing on final calculations
dat_all <- dat_all[!(dat_all$fips == "-998" & dat_all$deaths == 0 & dat_all$cases == 0),]
dat_all$fips <- as.numeric(dat_all$fips)
states_w_unknowns <- unique(dat_all$state[dat_all$fips == -999])

# Requires full state names are present
dat_population <- merge(dat_population, fips_state_df, by="fips")

for(s in 1:length(states_w_unknowns)){
  tmp_state <- states_w_unknowns[s]
  if(s == 1){
    dat_cleaned_1 <- fips_na_helper(dat_all, tmp_state, dat_population$POP[dat_population$full == tmp_state], 
                                    dat_population$fips[dat_population$full == tmp_state], 
                                    dat_population$county[dat_population$full == tmp_state], -999)
  }
  else{
    dat_cleaned_1 <- fips_na_helper(dat_cleaned_1, tmp_state, dat_population$POP[dat_population$full == tmp_state], 
                                    dat_population$fips[dat_population$full == tmp_state], 
                                    dat_population$county[dat_population$full == tmp_state], -999)
  }
}

# For New York City taking a slightly different approach to using the na_fips_helper
# There is one of the two known "mislabeled" set of cases in the documentation 
# See https://github.com/nytimes/covid-19-data for details
# Data Source: https://en.wikipedia.org/wiki/List_of_counties_in_New_York
NYC_POP <- c(1585873, 2504700, 2230722, 1385108, 468730)
NYC_FIPS <- c(36061, 36047, 36081, 36005, 36085)
NYC_COUNTY <- c("New York County", "Kings County", "Queens County", "Bronx County", "Richmond County")

dat_cleaned_2 <- fips_na_helper(dat_cleaned_1, "New York", NYC_POP, NYC_FIPS, NYC_COUNTY, -998)

# Similar Approach to Kansas City as used for New York
# Source: https://en.wikipedia.org/wiki/List_of_counties_in_Missouri
KC_POP <- c(99478, 221939, 674158, 89322)
KC_FIPS <- c(29037, 29047, 29095, 29165)
KC_COUNTY <- c("Cass County", "Clay County", "Jackson County", "Platte County")

dat_cleaned_3 <- fips_na_helper(dat_cleaned_2, "Missouri", KC_POP, KC_FIPS, KC_COUNTY, -998)

fips_counties <- unique(us_map[,c('fips', "county")])
plot_df <- merge(dat_cleaned_3[,c("fips", "date", "state", "cases", "deaths")], fips_counties, by="fips")
plot_df <- merge(plot_df, dat_population[,c("fips", "POP")], by="fips")

plot_df$cases_per_100k <- plot_df$cases/plot_df$POP*100000
plot_df$deaths_per_100k <- plot_df$deaths/plot_df$POP*100000

#----------------------------------------------------------------------------------------------------------------------
# Plotting the data... finally

# Dropping county to prevent merge problems (have slightly different naming structures across the data sets)
plot_map <- merge(plot_df, us_map[,-10], by="fips", all = TRUE)
plot_map$date <- as.Date(plot_map$date)

# Lots of valid 0's - creating a color scheme that applies 10 gradients to cases with > 0 cases per 100,000
plot_map$case_per_100k_grouping[plot_map$cases_per_100k > 0] <- cut_number(plot_map$cases_per_100k[plot_map$cases_per_100k > 0], 10)
plot_map$case_per_100k_grouping[plot_map$cases_per_100k == 0] <- 0

# Summary data for plotting break points on gradient scale: 
breaks_df <- plot_map %>% 
  select(case_per_100k_grouping, cases_per_100k) %>% 
  group_by(case_per_100k_grouping) %>% 
  summarize(mean(cases_per_100k))

# Sorting plot - relevant for getting geom_polygon to draw expected shape
plot_map <- plot_map[order(plot_map$date, plot_map$state, plot_map$fips, plot_map$order),]

breaks_vals <- unlist(breaks_df[c(4,7,10),1])
names(breaks_vals) <- NULL

breaks_labels <- unlist(round(breaks_df[c(4,7,10),2], 1))
breaks_labels <- as.character(breaks_labels)
names(breaks_labels) <- NULL

# Down-sample number of days... (R keeps crashing without helpful errors as to why...)
# unique_dates <- unique(plot_map$date)
# keep_dates <- unique_dates[seq(1, length(unique_dates), by=5)]
# plot_map <- plot_map[plot_map$date %in% keep_dates, ]

# plot_map <- plot_map[plot_map$date %in% keep_dates,]
map_figure <- ggplot(data = plot_map) +
  geom_polygon(aes(x=x, y=y, group=group, fill=case_per_100k_grouping), rule="winding", color="grey", lwd=.25) +
  labs(title = "Cumulative COVID-19 Cases per 100,000 Population - Daily Snapshots: {closest_state}",
       fill="Confirmed COVID-19 Cases per 100,000 Population", 
       caption = paste0("Data from New York Times and U.S. Census Bureau", "\n", 
                        "https://github.com/nytimes/covid-19-data", "\n", 
                        "https://www.census.gov/")) +
  scale_fill_gradientn(colors=blues9, na.value="grey90", breaks=breaks_vals, 
                       labels=breaks_labels,
                       guide = guide_colourbar(barwidth = 35, barheight = 0.7,
                                               title.position = "top")) +
  theme(legend.position = "bottom",
        legend.title=element_text(size=14), 
        legend.text=element_text(size=10), 
        axis.title = element_blank(), 
        axis.ticks = element_blank(), 
        axis.text = element_blank(),
        panel.background = element_blank())+
  transition_states(date)

anim_save("test.gif", path="~")

# Stores in temporary folder
animated_map <- animate(map_figure, height=600, width=720, nframes = length(unique(plot_map$date))*2+25,
                        end_pause=25, fps = 5)

anim_save("~/covid-19-data/Day-to-day-change-in-US-by-county.gif", animation = animated_map)