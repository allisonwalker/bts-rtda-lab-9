import com.holdenkarau.spark.testing.DatasetSuiteBase
import views._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.scalatest.FunSuite


class SurveyProcessingTest extends FunSuite with DatasetSuiteBase {
  def readFromTestFile(): DataFrame ={
    val surveyDataFrame = spark.read.option("header", "true").csv("data/survey_results_public.csv")
    surveyDataFrame
  }

  test("Test functionality: how many programmers participated in the survey?"){
    val surveyDataFrame = readFromTestFile();
    val  expectedDeveloperCount: Long =  88883;
    val surveyProcessing: SurveyProcessing = new SurveyProcessing(surveyDataFrame, spark);
    val realDeveloperCount: Long = surveyProcessing.developerCount();
    assert(realDeveloperCount == expectedDeveloperCount)
  }

  test("Test functionality: Percentage of developers who do open source") {
    import spark.implicits._

    val expectedOpenSourcePercentageSeq = Seq(
      DeveloperOpenSourcePercentageView(
        "Less than once a month but more than once per year", 20561, 23.13265753856193
      ),
      DeveloperOpenSourcePercentageView(
        "Once a month or more often", 11055, 12.437698997558588
      ),
      DeveloperOpenSourcePercentageView(
        "Never", 32295, 36.33428214619219
      ),
      DeveloperOpenSourcePercentageView(
        "Less than once per year", 24972, 28.095361317687296
      )
    )

    val expectedOpenSourcePercentageView: Dataset[DeveloperOpenSourcePercentageView] =
      sc.parallelize(expectedOpenSourcePercentageSeq).toDS()

    val surveyDataFrame = readFromTestFile();
    val surveyProcessing: SurveyProcessing = new SurveyProcessing(surveyDataFrame, spark);
    val realDevelopOpenSourcePercentageView: Dataset[DeveloperOpenSourcePercentageView] =
      surveyProcessing.createDeveloperOpenSourcePercentageView()

     assert(datasetsAreEquals(expectedOpenSourcePercentageView, realDevelopOpenSourcePercentageView))
  }

  test("Average year writing first line (Age1stCode) of code grouped by sex (Gender) and sorted by average descendant") {
    val expectedPercentage = Array(
      AgeGenderView("Woman", 17.00396704302716),
      AgeGenderView("NA", 15.563623545999295),
      AgeGenderView("Man", 15.32599837000815),
      AgeGenderView("Non-binary, genderqueer, or gender non-conforming", 14.308884297520661)
    )

    val surveyDataFrame = readFromTestFile();
    val surveyProcessing: SurveyProcessing = new SurveyProcessing(surveyDataFrame, spark);

    val realPercentage = surveyProcessing.createAgeGenderView();

    assert(expectedPercentage, realPercentage.take(4))
  }

  test("Average professional coding Experience") {
    val expectedAvg = Array(
      AvgProfessionalCodingExperienceView("Senior executive/VP", 14.545547073791349),
      AvgProfessionalCodingExperienceView("Engineering manager", 12.925037859666835)
    )

    val surveyDataFrame = readFromTestFile();
    val surveyProcessing: SurveyProcessing = new SurveyProcessing(surveyDataFrame, spark);

    val realAvg = surveyProcessing.createAvgProfessionalCodingExperienceView();

    assert(expectedAvg, realAvg.take(2))
  }

  test("Percentage of developers that are Students") {
    val expectedPercentage = Array(
      PercentageDevStudentsView("No", 65816, 74.04790567375089),
      PercentageDevStudentsView("Yes, full-time", 15769,  17.741300361148927),
      PercentageDevStudentsView("Yes, part-time", 5429,  6.108029656964773),
      PercentageDevStudentsView("NA", 1869,  2.102764308135414)
    )

    val surveyDataFrame = readFromTestFile();
    val surveyProcessing: SurveyProcessing = new SurveyProcessing(surveyDataFrame, spark);

    val realPercentage = surveyProcessing.createPercentageDevStudentsView();

    assert(expectedPercentage, realPercentage.take(4))
  }

  test("Percentage of developers by race and ethnicity") {
    val expectedAvg = Array(
      PercentageByEthnicityView("White or of European descent", 54284, 56.87761944677284),
      PercentageByEthnicityView("NA", 12215, 12.79861693210394)
    )

    val surveyDataFrame = readFromTestFile();
    val surveyProcessing: SurveyProcessing = new SurveyProcessing(surveyDataFrame, spark);

    val realAvg = surveyProcessing.createPercentageByEthnicityView();

    assert(expectedAvg, realAvg.take(2))
  }

  test("Percentage of use of social media") {
    val expectedAvg = Array(
      PercentageSocialMediaView("Reddit", 14374, 16.171821383166634),
      PercentageSocialMediaView("YouTube", 13830, 15.55978083548035)
    )

    val surveyDataFrame = readFromTestFile();
    val surveyProcessing: SurveyProcessing = new SurveyProcessing(surveyDataFrame, spark);

    val realAvg = surveyProcessing.createPercentageSocialMediaView();

    assert(expectedAvg, realAvg.take(2))
  }

  test("Average age per country") {
    val expectedAvg = Array(
      AgeAverageByCountryView("Papua New Guinea", 63.0),
      AgeAverageByCountryView("Saint Kitts and Nevis", 57.0)
    )

    val surveyDataFrame = readFromTestFile();
    val surveyProcessing: SurveyProcessing = new SurveyProcessing(surveyDataFrame, spark);

    val realAvg = surveyProcessing.createAgeAverageByCountry();

    assert(expectedAvg, realAvg.take(2))
  }

  test("Percentage of use of language") {
    val expectedAvg = Array(
      PercentageLanguageUsedView("JavaScript", 59219, 13.403452545046726),
      PercentageLanguageUsedView("HTML/CSS", 55466, 12.554009673644636)
    )

    val surveyDataFrame = readFromTestFile();
    val surveyProcessing: SurveyProcessing = new SurveyProcessing(surveyDataFrame, spark);

    val realAvg = surveyProcessing.createPercentageLanguageUsed();

    assert(expectedAvg, realAvg.take(2))
  }

  test("Percentage of use of platform") {
    val expectedAvg = Array(
      PercentagePlatformUsedView("Linux", 42753, 15.479729313834467),
      PercentagePlatformUsedView("Windows", 40630, 14.71104722524956)
    )

    val surveyDataFrame = readFromTestFile();
    val surveyProcessing: SurveyProcessing = new SurveyProcessing(surveyDataFrame, spark);

    val realAvg = surveyProcessing.createPercentagePlaformUsed();

    assert(expectedAvg, realAvg.take(2))
  }

  def datasetsAreEquals[T](d1: Dataset[T], d2: Dataset[T]): Boolean ={
    val d1_prime = d1.groupBy().count()
    val d2_prime = d2.groupBy().count()
    d1_prime.intersect(d2_prime).count() == d2_prime.intersect(d1_prime).count()
  }
}
