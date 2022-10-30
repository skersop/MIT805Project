# ---- Libraries ----
library(tidyverse) 
library(lubridate) # For datetime conversions
library(patchwork) # For graph stitching
library(corrplot)

# ---- Directory and other set-up ----
setwd("C:\\Users\\User\\Downloads\\2019-Oct.csv")
getwd()
options(scipen = 99999,
        dplyr.summarise.inform = FALSE)

# ---- The data ----
data <- read_csv('2019-Oct.csv')
# Replace NA category codes and brands with "None"
data <- data %>%
  replace_na(list(category_code = "None", brand = "None"))

# ---- Visualisation ----
# Get top 5 most popular smartphone brands
top_smartphone_brands <- data %>%
  filter(category_code == "electronics.smartphone") %>%
  count(brand) %>%
  slice_max(n = 5, order_by = n) %>%
  .$brand

# Get top 10 most popular categories (other than smartphones)
top_categories <- data %>%
  filter(category_code != "electronics.smartphone") %>% 
  count(category_id) %>%
  slice_max(n = 10, order_by = n)  %>%
  .$category_id

# For the first visualisation, filter on the top brands, smartphone events only
p1_data <- data %>%
  filter(
    category_code == "electronics.smartphone",
    brand %in% top_10_smartphone_brands
    ) 

# Create plot
p1 <- p1_data %>%
  ggplot() +
  geom_boxplot(aes(x = brand, y = price, fill = event_type)) +
  theme_bw() +
  labs(
    title = 'Boxplot of prices for purchased smartphones in Oct 2019, ten most popular brands',
    x ='Brand',
    y = 'Price')+ 
  scale_fill_discrete(
    name = "Event Type",
    labels = c("Add to cart", "Purchase", "View")
  ) +
  theme(
    legend.position = "top",
    plot.title = element_text(hjust = 0.5)
  )

p1

# For the second visualisation, split between smartphone and non-smartphone
p2_data <- data %>%
  filter(event_type == 'purchase') %>%
  mutate(
    event_time = lubridate::as_datetime(event_time),
    brand = if_else(brand %in% top_smartphone_brands, brand, 'other'),
    category = if_else(category_code == 'electronics.smartphone', 'Smartphone', 'Non-smartphone')
  )

p2 <- p2_data %>%
  ggplot() +
  geom_density(
    aes(
      x = hms::as_hms(event_time),
      color = brand
    ),
    size = 1
  ) +
  theme_bw() +
  facet_grid(
    rows = vars(wday(event_time, label = TRUE)),
    cols = vars(category)
  ) +
  labs(
    title = 'Density plots for purchased products based on purchase time, split between smartphone purchases and non-smartphone purchases',
    x = 'Event time (hh:mm:ss)',
    y = 'Density'
  )+
  theme(
    legend.position = 'top',
    plot.title = element_text(hjust = 0.5)
  ) + 
  scale_color_discrete(
    name = "Brand"
  ) 

p2

# Cleanup
rm(p2_data)
rm(p2)

# Visualisation 3
# Fin events that are part of sessions where smartphones were purchased
p3_data_a <- data %>%
  semi_join(
    data %>%
      #slice_sample(n = 1000) %>%
      filter(category_code == 'electronics.smartphone',
             event_type == 'purchase'),
    by = 'user_session') 

# summarise the data
p3_data_b <- p3_data_a%>%
  mutate(event_time = as_datetime(event_time)) %>%
  group_by(user_session) %>%
  summarise(
    num_categories = n_distinct(category_id),
    num_brands = n_distinct(brand),
    num_products = n_distinct(product_id),
    event_duration = as.numeric(max(event_time) - min(event_time)),
    price = sum(if_else(event_type == 'purchase', price, 0)),
    num_items_purchased = sum(if_else(event_type == 'purchase', 1, 0))
  )

# Calculate correlation matrix and plot
p3_data_b %>%
  select(-user_session) %>%
  rename("Distinct Categories" = num_categories,
         "Distinct Brands" = num_brands,
         "Distinct Products" = num_products,
         "User session length" = event_duration,
         "Basket price" = price,
         "# of items purchased" = num_items_purchased) %>%
  cor() %>%
  ggcorrplot(
    outline.col = "white"
  ) +
  #theme_bw() +
  labs(
    title = "Correlation plot for various user session attributes"
  )+
  theme(
    plot.title = element_text(hjust = 0.5),
    panel.background = element_rect(fill = "white", colour = "grey")
  )
