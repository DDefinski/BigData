package agh.wggios.analizadanych.transformation

import agh.wggios.analizadanych.caseclass.person
import agh.wggios.analizadanych.session.sparksession

class transformation extends sparksession{
  def Actor(per: person): Boolean = {
    per.category == "actor"
  }
}
