package com.gelerion.spark.skew

package object nulls

case class Region(region_id: Int, region_name: String)
case class Country(country_id: String, country_name: String/*, region_id: Int*/)
case class Location(location_id: Int, street_address: String, city: String, country_id: String) //postal_code, state_province
