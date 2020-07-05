package com.gelerion.spark.grouping.questions.uncomparable;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Entreprise implements Comparable<Entreprise>, Serializable {

    public Entreprise() {
    }

    public Entreprise(String sigle, String nomNaissance, String nomUsage) {
        this.sigle = sigle;
        this.nomNaissance = nomNaissance;
        this.nomUsage = nomUsage;
        this.etablissements = new HashMap<>();
    }

    /** Liste des établissements de l'entreprise. */
    public Map<String, Etablissement> etablissements = new HashMap<>();

    /** Sigle de l'entreprise */
    private String sigle;

    /** Nom de naissance */
    private String nomNaissance;

    /** Nom d'usage */
    private String nomUsage;

    public String getSigle() {
        return sigle;
    }

    public void setSigle(String sigle) {
        this.sigle = sigle;
    }

    public String getNomNaissance() {
        return nomNaissance;
    }

    public void setNomNaissance(String nomNaissance) {
        this.nomNaissance = nomNaissance;
    }

    public String getNomUsage() {
        return nomUsage;
    }

    public void setNomUsage(String nomUsage) {
        this.nomUsage = nomUsage;
    }

    public Map<String, Etablissement> getEtablissements() {
        return etablissements;
    }

    public void setEtablissements(Map<String, Etablissement> etablissements) {
        this.etablissements = etablissements;
    }

    @Override
    public int compareTo(Entreprise o) {
//        return getSiren().compareTo(o.getSiren());
        return sigle.compareTo(o.sigle);
    }

    public void ajouterEtablissement(Etablissement etablissement) {
        etablissements.put(etablissement.name, etablissement);
    }
}
