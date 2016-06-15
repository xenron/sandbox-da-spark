package com.github.giocode.graphxbook

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.math.Ordering 

object BallGameApp {
    def main(args: Array[String]){
        // Configure the program
        val conf = new SparkConf()
                                .setAppName("Aggregate Basketball Stats")
                                .setMaster("local")
                                .set("spark.driver.memory", "2G")
        val sc = new SparkContext(conf)

            

        abstract class GameResult(
            val season:     Int, 
            val day:        Int,
            val loc:        String
        )

        case class GameStats(
            val score: Int,
            val fieldGoalMade:   Int,
            val fieldGoalAttempt: Int, 
            val threePointerMade: Int,
            val threePointerAttempt: Int,
            val threeThrowsMade: Int,
            val threeThrowsAttempt: Int, 
            val offensiveRebound: Int,
            val defensiveRebound: Int,
            val assist: Int,
            val turnOver: Int,
            val steal: Int,
            val block: Int,
            val personalFoul: Int
        ){
            def fgPercent: Double = 100.0 * fieldGoalMade / fieldGoalAttempt
            def tpPercent: Double = 100.0 * threePointerMade / threePointerAttempt
            def ftPercent: Double = 100.0 * threeThrowsMade / threeThrowsAttempt
            override def toString: String = "Score: " + score 
        }

        // case class SummaryResult(
        //     override val season:    Int, 
        //     override val day:       Int,
        //     override val loc:       String, 
        //     val stats:          Int 
        // ) extends GameResult(season, day, loc)

        case class FullResult(
            override val season:    Int, 
            override val day:       Int,
            override val loc:       String, 
            val winnerStats:        GameStats,
            val loserStats:         GameStats 
        ) extends GameResult(season, day, loc)

        val teams: RDD[(VertexId, String)] =
            sc.textFile("./data/teams.csv").
            filter(! _.startsWith("#")).
            map {line =>
                val row = line split ','
                (row(0).toInt, row(1))
            }
        
        // // season,daynum,wteam,wscore,lteam,lscore,wloc,numot
        // val summaryStats: RDD[Edge[SummaryResult]] =
        //     sc.textFile("./data/regular_season_summary.csv").
        //     filter(! _.startsWith("#")).
        //     map {line =>
        //         val row = line split ','
        //         Edge(row(2).toInt, row(4).toInt,
        //             SummaryResult(
        //                 row(0).toInt, row(1).toInt, 
        //                 row(6), row(3).toInt - row(5).toInt
        //             )
        //         )
        //     }

        /* Parse detailed stats 
         0 to 7:   season,daynum,wteam,wscore,lteam,lscore,wloc,numot,
         8 to 20:  wfgm,wfga,wfgm3,wfga3,wftm,wfta,wor,wdr,wast,wto,wstl,wblk,wpf,
         21 to 33: lfgm,lfga,lfgm3,lfga3,lftm,lfta,lor,ldr,last,lto,lstl,lblk,lpf
        */
        val detailedStats: RDD[Edge[FullResult]] =
            sc.textFile("./data/regular_season_detailed.csv").
            filter(! _.startsWith("#")).
            map {line =>
                val row = line split ','
                Edge(row(2).toInt, row(4).toInt, 
                    FullResult(
                        row(0).toInt, row(1).toInt, 
                        row(6),
                        GameStats(      
                                        score = row(3).toInt,
                                fieldGoalMade = row(8).toInt,
                             fieldGoalAttempt = row(9).toInt, 
                             threePointerMade = row(10).toInt,
                          threePointerAttempt = row(11).toInt,   
                              threeThrowsMade = row(12).toInt,
                           threeThrowsAttempt = row(13).toInt, 
                             offensiveRebound = row(14).toInt,
                             defensiveRebound = row(15).toInt,
                                       assist = row(16).toInt,
                                     turnOver = row(17).toInt,
                                        steal = row(18).toInt,
                                        block = row(19).toInt,
                                 personalFoul = row(20).toInt
                        ),
                        GameStats(
                                        score = row(5).toInt,
                                fieldGoalMade = row(21).toInt,
                             fieldGoalAttempt = row(22).toInt, 
                             threePointerMade = row(23).toInt,
                          threePointerAttempt = row(24).toInt,
                              threeThrowsMade = row(25).toInt,
                           threeThrowsAttempt = row(26).toInt, 
                             offensiveRebound = row(27).toInt,
                             defensiveRebound = row(28).toInt,
                                       assist = row(20).toInt,
                                     turnOver = row(30).toInt,
                                        steal = row(31).toInt,
                                        block = row(32).toInt,
                                 personalFoul = row(33).toInt
                        )
                    )
                )
            }

        // Game Score graphs
        val fullScoreGraph = Graph(teams, detailedStats)

        // Score for 2015 season         
        val scoreGraph = fullScoreGraph subgraph (epred = _.attr.season == 2015)

        // Print to test
        scoreGraph.triplets.filter(_.dstAttr == "Duke").foreach(println)


        // Aggregate the total field goals made by winning teams
        type FGMsg = (Int, Int)
        val winningFieldGoalMade: VertexRDD[FGMsg] = scoreGraph aggregateMessages(
            // sendMsg
            triplet => triplet.sendToSrc(1, triplet.attr.winnerStats.fieldGoalMade)
            // mergeMsg
            ,(x, y) => (x._1 + y._1, x._2+ y._2)
        )

        // Average field goals made per Game by winning teams
        val avgWinningFieldGoalMade: VertexRDD[Double] = 
            winningFieldGoalMade mapValues (
                (id: VertexId, x: FGMsg) => x match {
                    case (count: Int, total: Int) => total/count
                })
      
        // Aggregate the points scored by winning teams
        type PtsMsg = (Int, Int)
        val winnerTotalPoints: VertexRDD[PtsMsg] = scoreGraph.aggregateMessages[PtsMsg](
            // sendMsg
            triplet => triplet.sendToSrc(1, triplet.attr.winnerStats.score), 
            // mergeMsg
            (x, y) => (x._1 + y._1, x._2+ y._2)
        )

        // Average Points Per Game by winning teams
        var winnersPPG: VertexRDD[Double] = 
            winnerTotalPoints mapValues (
                (id: VertexId, x: PtsMsg) => x match {
                    case (count: Int, total: Int) => total/count
                })

        // Abstract out the average stat function
        def averageWinnerStat[S <% Double](graph: Graph[String, FullResult])(getStat: GameStats => S): VertexRDD[Double] = {
            type Msg = (Int, Double)
            val winningScore: VertexRDD[Msg] = graph.aggregateMessages[Msg](
                // sendMsg
                triplet => triplet.sendToSrc(1, getStat(triplet.attr.winnerStats)), 
                // mergeMsg
                (x, y) => (x._1 + y._1, x._2+ y._2)
            )

            winningScore mapValues (
                (id: VertexId, x: Msg) => x match {
                    case (count: Int, total: Double) => total/count
                })
        }

        // Getting individual stats
        def score(stats: GameStats) = stats.score
        def threePointPercent(stats: GameStats) = stats.tpPercent

        // Getting average stats
        winnersPPG = averageWinnerStat(scoreGraph)(score) 
        var winnersThreePointPercent = averageWinnerStat(scoreGraph)(threePointPercent)  

        trait Teams
        case class Winners extends Teams 
        case class Losers extends Teams
        case class AllTeams extends Teams

        // Abstracting further 
        def averageStat(graph: Graph[String, FullResult])(getStat: GameStats => Double, tms: Teams): VertexRDD[Double] = {
            type Msg = (Int, Double)
            val aggrStats: VertexRDD[Msg] = graph.aggregateMessages[Msg](
                // sendMsg
                tms match {
                    case _ : Winners => t => t.sendToSrc((1, getStat(t.attr.winnerStats)))
                    case _ : Losers  => t => t.sendToDst((1, getStat(t.attr.loserStats)))
                    case _       => t => {
                        t.sendToSrc((1, getStat(t.attr.winnerStats)))
                        t.sendToDst((1, getStat(t.attr.loserStats)))
                    }
                }
                , 
                // mergeMsg
                (x, y) => (x._1 + y._1, x._2+ y._2)
            )

            aggrStats mapValues (
                (id: VertexId, x: Msg) => x match {
                    case (count: Int, total: Double) => total/count
                })
        }

        // Average Three Point Percent of Winning Teams 
        winnersThreePointPercent = averageStat(scoreGraph)(threePointPercent, Winners())    
        // Average Three Point Percent of Losing Teams
        val losersThreePointPercent = averageStat(scoreGraph)(threePointPercent, Losers())    
        
        // Number of wins of the teams
        val numWins: VertexRDD[Int] = scoreGraph.aggregateMessages(
            triplet => {
                triplet.sendToSrc(1) 
                triplet.sendToDst(0)
            },
            (x, y) => x + y
        )

        // Number of losses of the teams
        val numLosses: VertexRDD[Int] = scoreGraph.aggregateMessages(
            triplet => {
                triplet.sendToDst(1) 
                triplet.sendToSrc(0)
            },
            (x, y) => x + y
        )

        // Average Points Per Game of Winning Teams
        val winnersAvgPPG = averageStat(scoreGraph)(score, Winners())

        // Average Points Per Game of Losing Teams
        val losersAvgPPG = averageStat(scoreGraph)(score, Losers())

        // Average Points Per Game of All Teams
        val allAvgPPG = averageStat(scoreGraph)(score, AllTeams())

        // How about points conceided by all teams
        def averageConceidedStat(graph: Graph[String, FullResult])(getStat: GameStats => Double, rxs: Teams): VertexRDD[Double] = {
            type Msg = (Int, Double)
            val aggrStats: VertexRDD[Msg] = graph.aggregateMessages[Msg](
                // sendMsg
                rxs match {
                    case _ : Winners => t => t.sendToSrc((1, getStat(t.attr.loserStats)))
                    case _ : Losers  => t => t.sendToDst((1, getStat(t.attr.winnerStats)))
                    case _       => t => {
                        t.sendToSrc((1, getStat(t.attr.loserStats)))
                        t.sendToDst((1, getStat(t.attr.winnerStats)))
                    }
                }
                , 
                // mergeMsg
                (x, y) => (x._1 + y._1, x._2+ y._2)
            )

            aggrStats mapValues (
                (id: VertexId, x: Msg) => x match {
                    case (count: Int, total: Double) => total/count
                })
        }
        // Conceided points by all teams
        val avgConceidedPoints = averageConceidedStat(scoreGraph)(score, AllTeams())

        // Stat extractor functions
        def fouls (stats: GameStats) = stats.personalFoul
        def threeThrowPoints (stats: GameStats) = stats.threeThrowsMade
        def threeThrowPointsAttempts (stats: GameStats) = stats.threeThrowsAttempt

        // Average Personal Fouls and conceided three throws
        def avgPersonalFouls = averageStat(scoreGraph)(fouls, AllTeams())
        def avgConceidedFreeThrows = averageConceidedStat(scoreGraph)(threeThrowPoints, AllTeams())
        val avgConceidedFreeThrowsAttempt = averageConceidedStat(scoreGraph)(threeThrowPointsAttempts, AllTeams())        


        // Average Stats of All Teams 
        case class TeamStat(
                wins: Int  = 0      // Number of wins
             ,losses: Int  = 0      // Number of losses
                ,ppg: Int  = 0      // Points per game
                ,pcg: Int  = 0      // Points conceded per game
                ,fgp: Double  = 0   // Field goal percentage
                ,tpp: Double  = 0   // Three point percentage
                ,ftp: Double  = 0   // Free Throw percentage
             ){
            override def toString = wins + "-" + losses
        }

        // Aggregating average statistics for all teams 
        val teamStats: VertexRDD[TeamStat] = {
            type Msg = (Int, Int, Int, Int, Int, Double, Double, Double)

            val aggrStats: VertexRDD[Msg] = scoreGraph.aggregateMessages[Msg](
                // sendMsg
                t => {
                        t.sendToSrc((   1,
                                        1, 0, 
                                        t.attr.winnerStats.score, 
                                        t.attr.loserStats.score,
                                        t.attr.winnerStats.fgPercent,
                                        t.attr.winnerStats.tpPercent,
                                        t.attr.winnerStats.ftPercent
                                   ))
                        t.sendToDst((   1,
                                        0, 1, 
                                        t.attr.loserStats.score, 
                                        t.attr.winnerStats.score,
                                        t.attr.loserStats.fgPercent,
                                        t.attr.loserStats.tpPercent,
                                        t.attr.loserStats.ftPercent
                                   ))
                     }
                , 
                // mergeMsg
                (x, y) => ( x._1 + y._1, x._2 + y._2, 
                            x._3 + y._3, x._4 + y._4,
                            x._5 + y._5, x._6 + y._6,
                            x._7 + y._7, x._8 + y._8
                        )
            )

            // Return VertexRDD[TeamStat]
            aggrStats mapValues (
                (id: VertexId, m: Msg) => m match {
                    case ( count: Int, 
                            wins: Int, 
                          losses: Int,
                          totPts: Int, 
                      totConcPts: Int, 
                           totFG: Double,
                           totTP: Double, 
                           totFT: Double)  => TeamStat( wins, losses,
                                                        totPts/count,
                                                        totConcPts/count,
                                                        totFG/count,
                                                        totTP/count,
                                                        totFT/count)

                })
        }


        // Joining the average stats to vertex attributes
        case class Team(name: String, stats: Option[TeamStat]) {
            override def toString = name + ": " + stats
        }
        def addTeamStat(id: VertexId, t: Team, stats: TeamStat) = Team(t.name, Some(stats))
        
        val statsGraph: Graph[Team, FullResult] = 
            scoreGraph.mapVertices((_, name) => Team(name, None)).
                       joinVertices(teamStats)(addTeamStat)

        object winsOrdering extends Ordering[Option[TeamStat]] {
            def compare(x: Option[TeamStat], y: Option[TeamStat]) = (x, y) match {
                case (None, None)       => 0 
                case (Some(a), None)    => 1
                case (None, Some(b))    => -1
                case (Some(a), Some(b)) => if (a.wins == b.wins) a.losses compare b.losses
                                           else a.wins compare b.wins
            }
        }

        // Printing the top 10 Teams with most victories during regular season
        statsGraph.vertices.sortBy(v => v._2.stats,false)(winsOrdering, classTag[Option[TeamStat]]).
                            take(10).foreach(println)



    }
}
