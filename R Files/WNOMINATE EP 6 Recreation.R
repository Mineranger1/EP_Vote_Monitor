pacman::p_load(wnominate,dplyr,pscl,ggplot2,here)
here()
mepsEP6<-read.csv(here("Cleaned_data","EP6_clean_data","mep_info_for_wnominate.csv"),header=TRUE,strip.white=TRUE)
votesEP6 <- read.csv("./Cleaned_data/EP6_clean_data/wnominate_ep6_votes.csv",header = TRUE, strip.white = TRUE)

names <- mepsEP6[,1]
legData <- matrix(mepsEP6[,2],length(mepsEP6[,2]),1)
colnames(legData) <- "EPG"
matrix_df <- as.data.frame(votesEP6)
rc2 <- rollcall(votesEP6, yea = 1 , nay = 2 , missing = 3,notInLegis = 0,legis.names=names,legis.data = legData,desc="EP6")
result<-wnominate(rc2,polarity=c(10,10))
summary(result) 
legislators <- data.frame(
  EPG = result$legislators$EPG,
  coord1D = result$legislators$coord1D,
  coord2D = result$legislators$coord2D
)
ggplot(legislators, aes(x = coord1D, y = coord2D, color = EPG, label = EPG)) +
  geom_point(size = 3) +
  geom_text(vjust = 1.5, hjust = 1.5, check_overlap = TRUE) +
  labs(title = "Legislators on Cartesian Plane",
       x = "Coordinate 1D",
       y = "Coordinate 2D") +
  theme_minimal() +
  scale_color_discrete(name = "EPG Labels")
