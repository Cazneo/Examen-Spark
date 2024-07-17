import org.apache.spark.sql.SparkSession
import com.example.{FRvideo,RunningTotal,DiffSalary}

object Main extends App {
  val spark = SparkSession.builder().appName("Data Analysis").getOrCreate()

  val FRvideoPath = "C:/distrib/projet_traitement_distrib/Data/FRvideos.csv"
  val FRvideoData = FRvideo.loadData(spark, FRvideoPath)

  val top10videotrending = FRvideo.topvideotrending(FRvideoData)
  top10videotrending.show()


  val ratioLikedislikeByChaine = FRvideo.ratioLikedislikeByChaine(FRvideoData)
  ratioLikedislikeByChaine.show()

  val diffSalarypath = "C:/distrib/projet_traitement_distrib/Data/DiffSalary.csv"
  val diffSalarydata = DiffSalary.loadData(spark, diffSalarypath)

  val DifferenceSalaryBySalary = DiffSalary.DifféreSalaryBySalary(diffSalarydata)
  DifferenceSalaryBySalary.show




 // Load and analyze Trump tweet data
  //val trumpDataPath = "C:/distrib/projet_traitement_distrib/Data/trump_insult_tweets.csv"
  //val trumpData = TweetTrump.loadData(spark, trumpDataPath)

  //val top7Targets = TweetTrump.top7Targets(trumpData)
  //top7Targets.show()

  //val topInsults = TweetTrump.topInsults(trumpData)
  //topInsults.show()

  //val insultsForJoeBiden = TweetTrump.insultsForJoeBiden(trumpData)
  //insultsForJoeBiden.show()

  //val mexicoCount = TweetTrump.tweetCountByWord(trumpData, "mexico")
  //mexicoCount.show()

  //val chinaCount = TweetTrump.tweetCountByWord(trumpData, "china")
  //chinaCount.show()

  //val coronavirusCount = TweetTrump.tweetCountByWord(trumpData, "coronavirus")
  //coronavirusCount.show()

  //val tweetCountByPeriod = TweetTrump.tweetCountByPeriod(trumpData)
  //tweetCountByPeriod.show()

  // Load and analyze Airbnb listing data
  //val airbnbDataPath = "C:/distrib/projet_traitement_distrib/Data/listings.csv"
  //val airbnbData = RbnbStat.loadData(spark, airbnbDataPath)

  //val roomTypeAnalysis = RbnbStat.roomTypeAnalysis(airbnbData)
  //roomTypeAnalysis.show()

  //val averageNightsBooked = RbnbStat.averageNightsBooked(airbnbData)
  //averageNightsBooked.show()

  //val averagePricePerNight = RbnbStat.averagePricePerNight(airbnbData)
  //averagePricePerNight.show()

  //val averageIncome = RbnbStat.averageIncome(airbnbData)
  //averageIncome.show()

  //val rentalTypeAnalysis = RbnbStat.rentalTypeAnalysis(airbnbData)
  //rentalTypeAnalysis.show()

  //val listingTypeAnalysis = RbnbStat.listingTypeAnalysis(airbnbData)
  //listingTypeAnalysis.show()

  //val topHosts = RbnbStat.topHosts(airbnbData)
  //topHosts.show()

  // Load and analyze Netflix data
  //val netflixDataPath = "C:/distrib/projet_traitement_distrib/Data/netflix_titles.csv"
  //val netflixData = NetflixStats.loadData(spark, netflixDataPath)

  //val prolificDirectors = NetflixStats.prolificDirectors(netflixData)
  //prolificDirectors.show()

  //val countryProductionPercentage = NetflixStats.countryProductionPercentage(netflixData)
  //countryProductionPercentage.show()

  //val movieDurations = NetflixStats.movieDurations(netflixData)
  //movieDurations.show()

  //val averageDurationByInterval = NetflixStats.averageDurationByInterval(netflixData)
  //averageDurationByInterval.show()

  //val topDirectorActorDuo = NetflixStats.topDirectorActorDuo(netflixData)
  //topDirectorActorDuo.show()

  spark.stop()
}
