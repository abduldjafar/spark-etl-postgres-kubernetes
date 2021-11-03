from pydeequ.checks import *
from pydeequ.verification import *
from script.notification.discord import Discord


class QualityChecking(object):
    def __init__(self, spark_session):
        self.spark_session = spark_session
        self.notification = Discord(
            ""
        )

    def verification(self, dataFrame):

        check = Check(
            self.spark_session, CheckLevel.Warning, "final table verification"
        )

        checkResult = (
            VerificationSuite(self.spark_session)
            .onData(dataFrame)
            .addCheck(
                check.isUnique("customer_id")
                .isContainedIn(
                    "favourite_product", ["PURA100", "PURA500", "PURA250", "SUPA101"]
                )
                .isNonNegative("longest_streak")
            )
            .run()
        )
        checkResult_df = VerificationResult.successMetricsAsDataFrame(
            self.spark_session, checkResult
        )

        checkResult_df.repartition(1).write.mode("overwrite").csv("result")

        self.notification.dirname = "result"
        self.notification.run()
