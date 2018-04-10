mycon <- DBI::dbConnect(RMySQL::MySQL(), 
                        host = "172.21.100.3",
                        user = rstudioapi::askForPassword("Database username"),
                        password = rstudioapi::askForPassword("Database password")
                        #db = 'Laputa'
)


# dk标准间价格预估--数据提取 ---------------------------------------------------------
# dk间均价转换数据获取(北京&天津取近90天可入住房源，其他城市取近150天可入住房源)
{
  dk_std_room_convert_model_into_data<-dbGetQuery(mycon,
                                                  "select distinct o1.city,o1.block,o1.community,
                                                  before_room_num_x,bedroom_num_x,toilet_num_x,dw_num_x,xianfang_days_x,living_area_x,fix_area_x,has_balcony_x,price_x,face_x,has_toilet_x,
                                                  2 as before_room_num_y,
                                                  3 as bedroom_num_y,
                                                  1 as toilet_num_y,
                                                  0 as dw_num_y,
                                                  0 as xianfang_days_y,
                                                  12 as living_area_y,
                                                  12 as fix_area_y,
                                                  0 as has_balcony_y,
                                                  '' as price_y,
                                                  3 as face_y,
                                                  0 as has_toilet_y,
                                                  o1.code,o1.status,#o1.dts,
                                                  case when o1.city='北京市'  then 1
                                                  when o1.city='深圳市' or o1.city='上海市' then 2
                                                  when o1.city='杭州市' or o1.city='广州市'  then 3
                                                  else 4 end as city_level
                                                  from 
                                                  (
                                                  select distinct c.city,c.block,c.name as community,b.bedroom_num as bedroom_num_x,d.before_room_num as before_room_num_x,b.toilet_num as toilet_num_x,f.dw_num as dw_num_x,
                                                  case when datediff(CURDATE(),b.ready_for_rent_date)<0 then 0 else datediff(CURDATE(),b.ready_for_rent_date) end as xianfang_days_x,
                                                  a.suite_id,a.code,a.area as area_x,a.living_area as living_area_x,a.area - a.toilet_area as fix_area_x,
                                                  case when a.has_toilet='有' then 1 else 0 end as has_toilet_x,
                                                  case when a.has_balcony='有' then 1 else 0 end as has_balcony_x,a.price as price_x,
                                                  case when a.face='北' then 1 
                                                  when a.face in ('西','西北') then 2
                                                  when a.face in ('东','东北') then 3
                                                  else 4 end as face_x,
                                                  case when a.status= '可出租' then 0 else 1 end as status#,
                                                  #case when b1.dts is null then 1 else (b1.dts + 1) end as dts
                                                  from Laputa.rooms a
                                                  join Laputa.suites b on a.suite_id=b.id
                                                  join Laputa.xiaoqus c on b.xiaoqu_id=c.id
                                                  join DW.f_bi_sfinfor_day d on b.id=d.suite_id 
                                                  join 
                                                  (
                                                  select suite_id,sum(if(has_toilet='有',1,0)) as dw_num
                                                  from Laputa.rooms
                                                  where price is not null
                                                  group by suite_id
                                                  ) f
                                                  on  a.suite_id=f.suite_id
                                                  # join
                                                  # (
                                                  # select suite_id,min(available_date) as first_dt
                                                  # from DW.f_bi_roombackup
                                                  # where price is not null  and inserttime>='2018-02-01'
                                                  # group by suite_id
                                                  # ) oo
                                                  # on a.suite_id = oo.suite_id
                                                  # left join 
                                                  # (
                                                  # select code,price,count(distinct dt) as dts
                                                  # from 
                                                  # (
                                                  # select distinct substr(inserttime,1,10) as dt,code,price
                                                  # from DW.f_bi_roombackup
                                                  # where inserttime>='2018-02-01' and status='可出租'
                                                  # ) o
                                                  # group by code,price
                                                  # ) b1
                                                  # on a.code = b1.code and a.price = b1.price
                                                  where a.price is not null and a.rent_type not like '%整租%' and b.bedroom_num>1 and 
                                                  ((c.city in ('北京市', '天津市') and b.ready_for_rent_date>=date_sub(CURDATE(),interval 90 day))  or b.ready_for_rent_date>=date_sub(CURDATE(),interval 150 day)) 
                                                  and a.lighting='私有采光' and a.is_small_window='否' and a.shape='正常'  and a.has_storeroom!='有' and a.has_terrace!='有' and a.status not in ('已下架')
                                                  #and a.suite_id=19731
                                                  ) o1 
                                                  ") %>%
    tbl_df %>%
    as.data.frame  
}


# ziroom标准间价格预估--数据提取 -----------------------------------------------------
# ziroom间均价转换数据获取(ziroom近7天爬虫数据)
{
  ziroom_std_room_convert_model_into_data<-dbGetQuery(mycon,
                                                      "select distinct case when b.city is null then concat(a.city,'市') else b.city end as city,
                                                        case when b.company is not null then b.block else a.block end as block,
                                                      case when b.company is null then a.community_name else b.company end as community,
                                                      cast(bedroom_num as signed) as bedroomnum_x,
                                                      round(building_area,0) as area_x,
                                                      case when independent_balcon='独立阳台' then 1 else 0 end as balcony_x,
                                                      cast(price as signed) as price_x,
                                                      case when orientation='北' then 1 
                                                      when orientation in ('西','西北') then 2
                                                      when orientation in ('东','东北') then 3
                                                      else 4 end as face_x,
                                                      case when private_bathroom='独卫' then 1 else 0 end as bathroom_x,
                                                      case when style like '%4.0%' then 4 else 0 end as style_x,
                                                      3 as bedroomnum_y,
                                                      12 as area_y,
                                                      0  as balcony_y,
                                                      0  as price_y,
                                                      3  as face_y,
                                                      0  as bathroom_y,
                                                      4  as style_y,
                                                      case when a.city='北京' or a.city='上海' then 1
                                                      when a.city='深圳' then 2
                                                      when a.city='杭州' or a.city='广州'  then 3
                                                      else 4 end as city_level
                                                      from
                                                      (
                                                      select case when business_district like '%号线%' then replace(substring_index(business_district,'线',-1),')','') else substr(business_district_block,3,(length(business_district_block)-6)) end as block,o.*
                                                      from Monitor.archive_ziroom o
                                                      ) a
                                                      left join DW.xiaoqurelations b on concat(a.city,'市')=b.city and a.community_name=b.jingdui
                                                      where  substr(a.date_created,1,10)>=date_sub(CURDATE(), interval 7 day) and bedroom_num>1  and product_type='友家合租'
                                                      and style like '%4.0%' and a.price > 500
                                                      ") %>%
    tbl_df %>%
    as.data.frame 
}


# 待预测数据提取 -----------------------------------------------------------------
# 单间定价数据获取
{
  dk_room_data<-dbGetQuery(mycon,
                           "select distinct  a1.city,a1.block,a1.community,a1.address,
                               icontract.origin_bedroom_num,
                           a1.bedroom_num,a1.toilet_num,a1.total_area,
                           bb1.dw_num,
                           bb1.dly_num,
                           case when a1.has_lift='有' then 1 else 0 end as has_lift,
                           case when a1.heating='集中供暖' then 1 else 0 end as heating,
                           a1.floor,
                           datediff(CURDATE(),a1.ready_for_rent_date) as xianfang_days,
                           a1.code,
                           case when a1.rent_type like '%整租%' then a1.total_area else a1.area end as area,
                           a1.living_area,
                           a1.balcony_area,a1.terrace_area,a1.toilet_area,a1.storeroom_area,
                           case when has_toilet='有' then 1 else 0 end as has_toilet,
                           case when has_balcony='有' then 1 else 0 end as has_balcony,
                           case when has_storeroom='有' then 1 else 0 end as has_storeroom,
                           case when has_terrace='有' then 1 else 0 end as has_terrace,
                           face as face_old,
                           case when face='北' then 1 
                           when face in ('西','西北') then 2
                           when face in ('东','东北') then 3
                           else 4 end as face,
                           case when lighting='非私有采光' then 0 
                           when lighting='无采光' then -1
                           else 1 end as lighting,
                           case when is_small_window='否' then 1 else 0 end as is_small_window,
                           case when shape='狭长型' then -1 
                           when shape='斜顶' then 0
                           when shape='其他' and (shape_note like '%不规则%' or shape_note like '%异型%' or shape_note like '%异形%' or shape_note like '%刀把型%' or shape_note like '%手枪型%' or shape_note like '%L型%' or shape_note like '%T字型%') then -2
                           else 1 end as shape,
                           a1.rent_type,
                           a1.status,
                           a1.price,
                           case when a1.city='北京市' or a1.city='上海市' then 1
                           when a1.city='深圳市' then 2
                           when a1.city='杭州市' or a1.city='广州市'  then 3
                           else 4 end as city_level,
                           20 as dts,
                           case when mm.supplier_end_date is not null then mm.supplier_end_date else a1.zhuangxiu_end_date end as zhuangxiu_end_date,
                           house_pricing.sales_wish_sum_price_yuan as pre_total_outpr,
                           icost.总成本 as decoration_cost,
                           in_dk.month_price,
                           in_dk.imonths,
                           in_dk.free_days,
                           a1.suite_id       
                           from 
                           (
                           select c.city,c.block,c.name as community,b.bedroom_num,b.toilet_num,b.area as total_area,b.has_lift,b.heating,b.floor,b.ready_for_rent_date,
                           a.suite_id,a.code,a.area,a.living_area,a.has_toilet,a.has_balcony,a.has_storeroom,a.has_terrace,a.price,a.rent_type,a.face,
                           a.lighting,a.is_small_window,a.shape,a.shape_note,a.balcony_area,a.terrace_area,a.toilet_area,a.storeroom_area,b.zhuangxiu_end_date,a.available_date,
                           b.address,a.status
                           
                           from Laputa.rooms a
                           join Laputa.suites b on a.suite_id=b.id
                           join Laputa.xiaoqus c on b.xiaoqu_id=c.id
                           where a.status not in ('可出租','已出租','已预定')  and a.deleted_at is null  #and a.contract_signed_date>='2018-02-01'
                           ) a1
                           join
                           (SELECT
                           a.suite_id,
                           max(a.suite_status) as 收房状态,
                           sum(a.decoration_budget+
                           a.furniture_budget+
                           a.appliance_budget+
                           a.allocation_budget+
                           a.operation_budget+
                           a.follow_operation_budget+
                           if(a.construction_material_budget is null,0,a.construction_material_budget))as 总成本
                           ,sum(a.decoration_amount+
                           a.furniture_amount+
                           a.appliance_amount+
                           a.allocation_amount+
                           a.operation_amount+
                           a.follow_operation_amount+
                           if(a.construction_material_amount is null,0,a.construction_material_amount))as 决算总成本
                           ,avg(total_yuan) as total_yuan
                           from Laputa.suite_decorations a
                           WHERE
                           measure_approve_pass_at<=current_timestamp
                           and measure_approve_status in ('审批通过','特批通过')
                           #measure_second_approve_at<=CURDATE()
                           #and  suite_id=10829
                           GROUP BY a.suite_id
                           ) icost
                           on a1.suite_id=icost.suite_id
                           join 
                           (SELECT
                           a.suite_id,
                           count(DISTINCT a.code) as bed_num,
                           max(b.toilet_num) as gw_num,
                           sum(case when a.has_toilet='有' then 1 else 0 end) as dw_num,
                           sum(case when a.has_shower='有' AND a.has_toilet='无' then 1 else 0 end) as dly_num,
                           '0' as gly_num, #公共淋浴间
                           '1' as gcf_num, #公共厨房
                           '0' AS dcf_num, #独立厨房
                           max(a.available_date) as available_date
                           from Laputa.rooms a,Laputa.suites b
                           WHERE
                           a.suite_id=b.id
                           and a.deleted_at is NULL
                           and a.status not in('已下架')
                           AND b.status not in ('已下架')
                           and b.address not like '%工作站%'
                           AND b.ready_for_rent_date
                           group by a.suite_id
                           )bb1
                           ON a1.suite_id=bb1.suite_id
                           join 
                           (SELECT *
                           from Laputa.contract_with_landlords
                           where
                           #manage_status not in ('作废')
                           #and status in ('已签约','签约待确认')
                           #and
                           status='已签约'
                           and 
                           sign_date<=CURDATE()
                           and ((suite_id is not NULL and type like '蛋壳%') or (type like '蓝鲸%'))
                           )icontract
                           on a1.suite_id=icontract.suite_id
                           LEFT JOIN
                           (SELECT
                           b.suite_id,
                           round(max(monthly_price),0) as month_price,
                           max(b.months) as imonths,
                           max(b.free_days) as free_days
                           FROM Laputa.landlord_payment_schedules a,Laputa.contract_with_landlords b
                           WHERE
                           a.monthly_price >0
                           and a.deleted_at is NULL
                           and a.contract_id=b.id
                           and (left(CURDATE(),7)>=left(a.date,7) and left(CURDATE(),7)<=left(a.end_date,7))
                           and b.suite_id is not null
                           and b.type like '蛋壳%'
                           AND b.status in ('已签约','签约待确认')
                           GROUP BY b.suite_id
                           )in_dk
                           on a1.suite_id=in_dk.suite_id
                           LEFT JOIN Forecast.house_pricing ON icontract.house_pricing_id=Forecast.house_pricing.id
                           left join
                           (
                           select suite_id,min(available_date)
                           from Laputa.rooms
                           where available_date is not null 
                           group by suite_id
                           ) cc
                           on a1.suite_id=cc.suite_id
                           left join  Laputa.suite_decorations mm on a1.suite_id = mm.suite_id 
                           where ((a1.city IN ('杭州市','上海市','深圳市','武汉市','南京市','广州市') and 
                           (case when mm.supplier_end_date is not null then mm.supplier_end_date else a1.zhuangxiu_end_date end) <=date_add(CURDATE(),interval 2 day)) or (a1.city in ('北京市' ,'天津市') ))
                           and a1.suite_id not in ('7736','5056','9077','8991','8866','9034','9032','10086','10036','8870','10111','10086','10036','8782','9935','10392','10420','10425','10293','8812','10204','10315','10499','10610','10477','10392','10315','10204','10036','8870','10432','10441','107171','10509','10541','10420','10293','9032','9034','10662','10373','8866','10804','10842','10675','10559','10432','10425','8991','8812','11023','10717','10895','10573','11138','11041','10972','10829','11092','11184','10510','10807','11010','10585','11017','10905','11473','11915','13395','13936','14799','14500','14506','14509','14376','14377','14582','14989','21393','22565','22504','20812','21878','23263','22623'
                           )
                           and a1.price is null and a1.available_date is NULL and (a1.rent_type not like '%整租%' or (a1.rent_type like '%整租%' and a1.code like '%A%'))
                           and a1.address not like '%研发%'
                           and cc.suite_id  is null
                           ") %>%
    tbl_df %>%
    as.data.frame
}



# load data
# connect with mysql db Forecast
Forecast <- DBI::dbConnect(RMySQL::MySQL(), 
                        host = "172.21.100.3",
                        dbname = "Forecast",
                        user = rstudioapi::askForPassword("Database username"),
                        password = rstudioapi::askForPassword("Database password"))

# get puzu price data from mysql
start_date <- Sys.Date() - 180
puzu_data <- tbl(Forecast, "community_house_type") %>% 
  filter(data_date >= start_date) %>% 
  filter(bedroom_num < 6) %>% 
  select(city, block, community = xiaoqu_name, 
         bedroom_num, area, price) %>% 
  collect()

# set city level
city_level <- tibble::tribble(
  ~city, ~city_level,
  "北京市",          1L,
  "上海市",          1L,
  "深圳市",          2L,
  "杭州市",          3L,
  "广州市",          3L,
  "天津市",          4L,
  "南京市",          4L,
  "武汉市",          4L
)
puzu_data <- puzu_data %>% 
  left_join(city_level, by = "city") %>% 
  mutate(puzu_avg_price = price / as.numeric(bedroom_num), 
         puzu_avg_area = area / as.numeric(bedroom_num),
         avg_price_area = price / area) %>% 
  group_by(city_level, city, community, bedroom_num) %>% 
  mutate(room_avg_price = sum(price) / sum(bedroom_num),
         room_median_price = median(puzu_avg_price)) %>% 
  group_by(city_level, city, community) %>% 
  mutate(com_avg_price = sum(price) / sum(bedroom_num),
         com_median_price = median(puzu_avg_price)) %>% 
  select(city_level, city, block, community, bedroom_num, 
         puzu_avg_price, puzu_avg_area, avg_price_area, 
         room_avg_price, room_median_price, 
         com_avg_price, com_median_price) %>% 
  unique()
