package org.hooray.iterator

interface LevelParticipation {
    // Returns a list of levels in which the participant variables are involved
    fun variableLevels(): List<Int>
    // Returns true if the participant is involved in the specified level
    // This returns true for a subset of variable levels, but not necessarily all of them
    // For example:
    //    [(= x y)]
    // variableLeves() = [1 2]
    // participatesInLevel(1) = false
    // participatesInLevel(2) = true
    fun participatesInLevel(level: Int): Boolean
}