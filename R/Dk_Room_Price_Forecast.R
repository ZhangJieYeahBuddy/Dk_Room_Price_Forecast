source("header.R")
# source("R/get_data_sql.R")


# ######dk标准间价格预估 ---------------------------------------------------------

# load model
std_room_outprice_pre_model<-xgb.load("Results/std_room_outprice_pre_model.model")

# calculate dev var
# dk_std_room_convert_model_into_data from get_data_sql.R--dk标准间价格预估--数据提取
dk_std_room_convert_model_into_data2 <- dk_std_room_convert_model_into_data %>% 
  mutate(area_xy_dev = living_area_y - living_area_x, 
         face_xy_dev = face_y - face_x, 
         has_toilet_xy_dev = has_toilet_y - has_toilet_x, 
         has_balcony_xy_dev = has_balcony_y - has_balcony_x, 
         bedroom_xy_dev = bedroom_num_y - bedroom_num_x)

dk_forcast_colnames <- c("bedroom_num_x","toilet_num_x","living_area_x","has_balcony_x","price_x","face_x","has_toilet_x",
                        "bedroom_num_y","toilet_num_y","living_area_y","has_balcony_y","face_y","has_toilet_y",
                        "city_level","area_xy_dev","face_xy_dev","bedroom_xy_dev","has_toilet_xy_dev","has_balcony_xy_dev")

# as.matrix
real_dk_std_room_convert_model_into_data2 <- dk_std_room_convert_model_into_data2 %>% 
  select(dk_forcast_colnames) %>% 
  sapply(as.numeric) 

# predict and calaulate quantile
dk_data <- dk_std_room_convert_model_into_data %>% 
  mutate(std_room_outprice_pre = price_x + 
           predict(std_room_outprice_pre_model, real_dk_std_room_convert_model_into_data2)) %>% 
  group_by(city, block, community) %>%
  mutate(final_std_room_outprice_pre = quantile(std_room_outprice_pre, 0.5))



# #####ziroom标准间价格预估 ------------------------------------------------------

# load model
zir_std_room_outprice_pre_model <- xgb.load("Results/zir_std_room_outprice_pre_model.model")

# calculate dev var
# ziroom_std_room_convert_model_into_data from get_date_sql.R--ziroom标准间价格预估--数据提取
ziroom_std_room_convert_model_into_data2 <- ziroom_std_room_convert_model_into_data %>% 
  mutate(area_xy_dev = area_y - area_x, 
         face_xy_dev = face_y - face_x, 
         bathroom_xy_dev = bathroom_y - bathroom_x, 
         style_xy_dev = style_y - style_x, 
         balcony_xy_dev = balcony_y - balcony_x, 
         bedroomnum_xy_dev = bedroomnum_y - bedroomnum_x)

ziroom_forcast_colnames <- c("area_y","bathroom_y","face_y","balcony_y","style_y","bedroomnum_y",
                            "area_x","bathroom_x","face_x","balcony_x","style_x","bedroomnum_x","price_x",
                            "city_level","area_xy_dev","face_xy_dev","bathroom_xy_dev","balcony_xy_dev","style_xy_dev","bedroomnum_xy_dev")

# as.matrix
real_ziroom_std_room_convert_model_into_data2 <- ziroom_std_room_convert_model_into_data2 %>% 
  select(ziroom_forcast_colnames)  %>% 
  sapply(as.numeric) 

# predict and calculate quantile
ziroom_data <- ziroom_std_room_convert_model_into_data %>% 
  mutate(std_room_outprice_pre = price_x + 
           predict(zir_std_room_outprice_pre_model, real_ziroom_std_room_convert_model_into_data2)) %>% 
  group_by(city, block, community) %>%
  mutate(final_std_room_outprice_pre = ifelse(city == '北京', 
                                              quantile(std_room_outprice_pre, 0.65), 
                                              quantile(std_room_outprice_pre, 0.6)))



# #######小区间均价处理--策略 ----------------------------------------------------------

xiaoqu_std_price <- dk_data %>% 
  full_join(ziroom_data, 
            by =c("city", "community"), 
            copy = FALSE, 
            suffix = c(".dk_data", ".ziroom_data")) %>%  
  select(city, block.dk_data, community, 
         final_std_room_outprice_pre.dk_data, final_std_room_outprice_pre.ziroom_data) %>% 
  distinct() %>%
  group_by(city, block.dk_data) %>%
  mutate(block_price.danke = mean(final_std_room_outprice_pre.dk_data, na.rm = TRUE),
         block_price.ziroom = mean(final_std_room_outprice_pre.ziroom_data, na.rm = TRUE)) %>% 
  mutate(xiaoqu_std_price_final = ifelse(is.na(final_std_room_outprice_pre.dk_data),
                                         final_std_room_outprice_pre.ziroom_data,
                                         final_std_room_outprice_pre.dk_data),
         block_std_price=ifelse(block_price.danke == 'NaN',
                                block_price.ziroom,
                                block_price.danke)) %>% 
  select(city, block.dk_data, community, xiaoqu_std_price_final, block_std_price) %>% 
  distinct()

# dk_room_data from get_data_sql.R--待预测数据提取
dk_room_price_data <- dk_room_data %>% 
  left_join(xiaoqu_std_price %>% 
              ungroup() %>% 
              select(city, community, xiaoqu_std_price_final) %>% 
              distinct(), 
            by =c("city", "community"), 
            copy = FALSE, 
            suffix = c(".dk_room_data", ".xiaoqu_std_price")) %>% 
  distinct() %>% 
  left_join(xiaoqu_std_price %>% 
              select(city, block.dk_data, block_std_price) %>% 
              distinct(),  
             by =c("city" = "city","block" = "block.dk_data"), 
             copy = FALSE, 
             suffix = c(".dk_room_data", ".xiaoqu_std_price")) %>% 
  distinct()


# 间均价缺失小区 -----------------------------------------------------------------

dk_room_price_data %>% 
  filter(is.na(xiaoqu_std_price_final)) %>% 
  select(city, block, community) %>% 
  distinct() %>% 
  write.csv(paste("Results/", Sys.Date(), "间均价缺失小区.csv"))


# ####单间价格预估 --------------------------------------------------------------

# load model
room_outprice_pre_model_has_balcony_area<-xgb.load("Results/room_outprice_pre_model_has_balcony_area.model")

# calculate dev var
dk_room_price_data <- dk_room_price_data %>% 
  filter(!is.na(xiaoqu_std_price_final)) %>% 
  mutate(bedroom_num_dev = 3 - bedroom_num, 
         living_area_dev = 12 - living_area, 
         face_dev = 3 - face) 

room_forcast_colnames <- c("bedroom_num","toilet_num","total_area","has_lift","heating","floor","xianfang_days",
                          "living_area","has_toilet","has_balcony","has_storeroom","has_terrace","face","lighting",
                          "is_small_window","shape","balcony_area",
                          "dts","city_level","xiaoqu_std_price_final","block_std_price",
                          "bedroom_num_dev","living_area_dev","face_dev")

# as.matrix
dk_room_price_data1 <- dk_room_price_data %>% 
  select(room_forcast_colnames) %>% 
  sapply(as.numeric)

# predict and adjust to 30,60,90
dk_room_price_data <- dk_room_price_data %>% 
  mutate(room_outprice_pre = xiaoqu_std_price_final - 
           predict(room_outprice_pre_model_has_balcony_area, dk_room_price_data1)) %>% 
  mutate(adj_room_outprice_pre = ifelse(room_outprice_pre %% 100 <= 30, 
                                        room_outprice_pre - room_outprice_pre %% 100 + 30, 
                                        room_outprice_pre),
         adj_room_outprice_pre = ifelse(room_outprice_pre %% 100 > 30 & room_outprice_pre %% 100 <= 60, 
                                        room_outprice_pre - room_outprice_pre %% 100 + 60,
                                        adj_room_outprice_pre),
         adj_room_outprice_pre = ifelse(room_outprice_pre %% 100 > 60, 
                                        room_outprice_pre - room_outprice_pre %% 100 + 90, 
                                        adj_room_outprice_pre))

# output
write.csv(dk_room_price_data, 
          paste("Results/", Sys.Date(), "新房定价结果.csv"), 
          row.names = F)
