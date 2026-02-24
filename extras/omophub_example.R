# install.packages("omophub")
library(omophub)

client <- OMOPHubClient$new(api_key = Sys.getenv("OMOPHUB_API_KEY"))
results <- client$search$basic("diabetes", page_size = 5)
print(results)
