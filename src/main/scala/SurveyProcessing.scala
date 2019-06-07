import views.{AgeAverageByCountryView, AgeGenderView, AvgProfessionalCodingExperienceView, DeveloperOpenSourcePercentageView, PercentageByEthnicityView, PercentageDevStudentsView, PercentageLanguageUsedView, PercentagePlatformUsedView, PercentageSocialMediaView}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._

class SurveyProcessing(surveyDataFrame: DataFrame, spark: SparkSession) {

  def developerCount(): Long = {
    this.surveyDataFrame.count()
  }

  def createDeveloperOpenSourcePercentageView(): Dataset[DeveloperOpenSourcePercentageView] = {
    import spark.implicits._
    surveyDataFrame.groupBy("OpenSourcer")
      .count()
      .withColumn("percentage", (col("count")/
        sum("count").over())*100)
      .as[DeveloperOpenSourcePercentageView]
  }

  def createAgeGenderView(): Dataset[AgeGenderView] = {
    import spark.implicits._
    surveyDataFrame.withColumn("Gender", explode(split($"Gender", ";")))
      .groupBy("Gender").agg(avg("Age1stCode").as("avg"))
      .orderBy(desc("avg"))
      .as[AgeGenderView]
  }

  def createPercentageDevStudentsView(): Dataset[PercentageDevStudentsView] = {
    import spark.implicits._
    surveyDataFrame.groupBy("Student")
      .agg(count("Student").alias("count"))
      .withColumn("percentage", col("count")/sum("count").over()*100)
      .orderBy(desc("percentage"))
      .as[PercentageDevStudentsView]
  }

  def createAvgProfessionalCodingExperienceView(): Dataset[AvgProfessionalCodingExperienceView] = {
    import spark.implicits._
    surveyDataFrame.withColumn("DevType", explode(split($"DevType", ";")))
      .groupBy("DevType")
      .agg(avg("YearsCodePro").as("avg"))
      .orderBy(desc("avg"))
      .as[AvgProfessionalCodingExperienceView]
  }

  def createPercentageByEthnicityView(): Dataset[PercentageByEthnicityView] = {
    import spark.implicits._
    surveyDataFrame.withColumn("Ethnicity", explode(split($"Ethnicity", ";")))
      .groupBy("Ethnicity")
      .agg(count("Ethnicity").alias("count"))
      .withColumn("percentage", col("count")/sum("count").over()*100)
      .orderBy(desc("percentage"))
      .as[PercentageByEthnicityView]
  }

  def createPercentageSocialMediaView(): Dataset[PercentageSocialMediaView] = {
    import spark.implicits._
    surveyDataFrame.groupBy("SocialMedia")
      .agg(count("SocialMedia").alias("count"))
      .withColumn("percentage", col("count")/sum("count").over()*100)
      .orderBy(desc("percentage"))
      .as[PercentageSocialMediaView]
  }

  def createAgeAverageByCountry(): Dataset[AgeAverageByCountryView] = {
    import spark.implicits._
    surveyDataFrame.groupBy("Country")
      .agg(avg("Age").alias("avg"))
      .orderBy(desc("avg"))
      .as[AgeAverageByCountryView]
  }

  def createPercentageLanguageUsed(): Dataset[PercentageLanguageUsedView] = {
    import spark.implicits._
    surveyDataFrame.withColumn("LanguageWorkedWith", explode(split($"LanguageWorkedWith", ";")))
      .groupBy("LanguageWorkedWith")
      .agg(count("LanguageWorkedWith").alias("count"))
      .withColumn("percentage", col("count")/sum("count").over()*100)
      .orderBy(desc("percentage"))
      .as[PercentageLanguageUsedView]
  }

  def createPercentagePlaformUsed(): Dataset[PercentagePlatformUsedView] = {
    import spark.implicits._
    surveyDataFrame.withColumn("PlatformWorkedWith", explode(split($"PlatformWorkedWith", ";")))
      .groupBy("PlatformWorkedWith")
      .agg(count("PlatformWorkedWith").alias("count"))
      .withColumn("percentage", col("count")/sum("count").over()*100)
      .orderBy(desc("percentage"))
      .as[PercentagePlatformUsedView]
  }
}
