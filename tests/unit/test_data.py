SPARSE_FITS_XML = '''<?xml version="1.0" encoding="UTF-8"?>
<fits xmlns="http://hul.harvard.edu/ois/xml/ns/fits/fits_output" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://hul.harvard.edu/ois/xml/ns/fits/fits_output http://hul.harvard.edu/ois/xml/xsd/fits/fits_output.xsd" version="1.2.0" timestamp="3/28/18 10:11 AM">
</fits>'''

FITS_XML = '''<?xml version="1.0" encoding="UTF-8"?>
<fits xmlns="http://hul.harvard.edu/ois/xml/ns/fits/fits_output" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://hul.harvard.edu/ois/xml/ns/fits/fits_output http://hul.harvard.edu/ois/xml/xsd/fits/fits_output.xsd" version="1.2.0" timestamp="3/28/18 10:11 AM">
  <identification status="SINGLE_RESULT">
    <identity format="TIFF EXIF" mimetype="image/tiff" toolname="FITS" toolversion="1.2.0">
      <tool toolname="Exiftool" toolversion="10.00" />
    </identity>
  </identification>
  <fileinfo>
    <size toolname="Jhove" toolversion="1.16">63478567</size>
    <creatingApplicationName toolname="Exiftool" toolversion="10.00" status="SINGLE_RESULT">GraphicsMagick 1.3.18 2013-03-10 Q8 http://www.GraphicsMagick.org/</creatingApplicationName>
    <filename toolname="OIS File Information" toolversion="0.2" status="SINGLE_RESULT">PBRU00025472.tif</filename>
    <md5checksum toolname="OIS File Information" toolversion="0.2" status="SINGLE_RESULT">d047452cd1517249070afe740effff6c</md5checksum>
    <fslastmodified toolname="OIS File Information" toolversion="0.2" status="SINGLE_RESULT">1522246251000</fslastmodified>
  </fileinfo>
  <filestatus />
  <metadata>
    <image>
      <compressionScheme toolname="Exiftool" toolversion="10.00" status="SINGLE_RESULT">Uncompressed</compressionScheme>
      <imageWidth toolname="Exiftool" toolversion="10.00" status="SINGLE_RESULT">3753</imageWidth>
      <imageHeight toolname="Exiftool" toolversion="10.00" status="SINGLE_RESULT">5634</imageHeight>
      <colorSpace toolname="Exiftool" toolversion="10.00" status="SINGLE_RESULT">RGB</colorSpace>
      <bitsPerSample toolname="Exiftool" toolversion="10.00" status="SINGLE_RESULT">8 8 8</bitsPerSample>
      <samplesPerPixel toolname="Exiftool" toolversion="10.00" status="SINGLE_RESULT">3</samplesPerPixel>
      <scanningSoftwareName toolname="Exiftool" toolversion="10.00" status="SINGLE_RESULT">GraphicsMagick 1.3.18 2013-03-10 Q8 http://www.GraphicsMagick.org/</scanningSoftwareName>
    </image>
  </metadata>
  <statistics fitsExecutionTime="1811">
    <tool toolname="MediaInfo" toolversion="0.7.75" status="did not run" />
    <tool toolname="OIS Audio Information" toolversion="0.1" status="did not run" />
    <tool toolname="ADL Tool" toolversion="0.1" status="did not run" />
    <tool toolname="VTT Tool" toolversion="0.1" status="did not run" />
    <tool toolname="Droid" toolversion="6.3" executionTime="1205" />
    <tool toolname="Jhove" toolversion="1.16" executionTime="1448" />
    <tool toolname="file utility" toolversion="5.14" executionTime="1072" />
    <tool toolname="Exiftool" toolversion="10.00" executionTime="1275" />
    <tool toolname="NLNZ Metadata Extractor" toolversion="3.6GA" status="did not run" />
    <tool toolname="OIS File Information" toolversion="0.2" executionTime="628" />
    <tool toolname="OIS XML Metadata" toolversion="0.2" status="did not run" />
    <tool toolname="ffident" toolversion="0.2" executionTime="912" />
    <tool toolname="Tika" toolversion="1.10" executionTime="1737" />
  </statistics>
</fits>'''

TEI_MINIMAL_SAMPLE = '''
<TEI xmlns:xi="http://www.w3.org/2001/XInclude"
     xmlns="http://www.tei-c.org/ns/1.0">
<teiHeader>
  <fileDesc>
    <titleStmt>
      <title>Minimal Title</title>
    </titleStmt>
    <publicationStmt>
      <p>Publication info</p>
    </publicationStmt>
    <sourceDesc>
      <p>Information about source from which the resource derives</p>
    </sourceDesc>
  </fileDesc>
</teiHeader>
<text>
  <p>Some text</p>
</text>
</TEI>
'''

TEI_IIP_SAMPLE='''<?xml version="1.1" encoding="UTF-8"?>
<?oxygen RNGSchema="http://www.stoa.org/epidoc/schema/latest/tei-epidoc.rng"?>
<TEI xmlns:xi="http://www.w3.org/2001/XInclude"
     xmlns="http://www.tei-c.org/ns/1.0"
     xml:id="elus0018"
     n="iip">
   <teiHeader>
      <fileDesc><!-- ********** <titleStmt> ********** -->
         <titleStmt>
            <title>ELUS0018: Elusa
                           (Haluza),
            Negev, 400 CE - 640 CE.
            Funerary (Epitaph).
            Tombstone.</title>
            <principal>
               <persName xml:id="MS">Michael Satlow</persName>
            </principal>
            <respStmt>
               <resp>Project Manager</resp>
               <persName>Gaia Lembi</persName>
            </respStmt>
            <respStmt>
               <resp>Technical Oversight</resp>
               <persName>Elli Mylonas</persName>
            </respStmt>
         </titleStmt>
         <editionStmt>
            <edition n="v1">First archival edition <date>2019-03-15</date>
            </edition>
         </editionStmt>
         <publicationStmt>
            <authority>Brown University</authority>
            <idno type="IIP">elus0018</idno>
            <availability status="free">
               <licence>This work is licensed under a Creative Commons Attribution-NonCommercial 4.0 International License.
                    <ref target="http://creativecommons.org/licenses/by-nc/4.0/">Distributed under a Creative Commons licence Attribution-BY 4.0</ref>
                  <p>
                        All reuse or distribution of this work must contain somewhere a link to the DOI of the Inscriptions
                        of Israel/Palestine Project:
                        <ref>https://doi.org/10.26300/pz1d-st89</ref>
                  </p>
               </licence>
            </availability>
         </publicationStmt>
         <!-- ********* <sourceDesc> ********* -->
         <sourceDesc>
            <msDesc>
               <msIdentifier>
                  <idno>Elus 0018</idno>
               </msIdentifier>
               <!-- ********* <msContents> ********* -->
               <msContents>
                  <textLang mainLang="grc"/>
                  <msItem class="#funerary.epitaph" ana="#christian">
                     <p>Negev. Elusa (Haluza), 400 CE - 640 CE. Tombstone. Epitaph.</p>
                  </msItem>
               </msContents>
               <!-- ********* <physDesc> ********* -->
               <physDesc><!-- ********* <objectDesc> ********* -->
                  <objectDesc ana="#tombstone">
                     <supportDesc ana="limestone">
                        <support>
                           <dimensions type="surface" unit="cm">
                              <height>26</height>
                              <width>25.5</width>
                              <depth>4</depth>
                           </dimensions>
                        </support>
                        <condition>
                           <p/>
                        </condition>
                     </supportDesc>
                     <!-- ********* <layoutDesc> ********* -->
                     <layoutDesc>
                        <layout columns="0" writtenLines="0">
                           <p/>
                        </layout>
                     </layoutDesc>
                  </objectDesc>
                  <!-- ********* <handDesc> ********* -->
                  <handDesc>
                     <handNote ana="#engraved">
                        <dimensions type="letter"
                                    extent="height"
                                    unit="cm"
                                    atLeast="2.5"
                                    atMost="5"/>/&gt;
                     </handNote>
                  </handDesc>
                  <!-- ********* <decoDesc> ********* -->
                  <decoDesc>
                     <decoNote ana="#xx">
                        <ab>cross</ab>
                        <locus>face of tombstone</locus>
                     </decoNote>
                  </decoDesc>
               </physDesc>
               <!-- ********* <history> ********* -->
               <history>
                  <origin>
                     <date period="http://n2t.net/ark:/99152/p0m63njjcn4"
                           notBefore="0400"
                           notAfter="0640">400 CE - 640 CE</date>
                     <placeName>
                        <region>Negev</region>
                        <settlement ref="http://pleiades.stoa.org/places/687890">Elusa
                           (Haluza)</settlement>
                        <geogName type="site"/>
                     </placeName>
                     <p>Negev, Elusa </p>
                  </origin>
                  <provenance>
                     <placeName/>
                     <date/>
                  </provenance>
               </history>
            </msDesc>
         </sourceDesc>
      </fileDesc>
      <!-- ********* <encodingDesc> ********* -->
      <encodingDesc>
         <projectDesc>
            <p>The Inscriptions of Israel/Palestine project seeks to build an internet-accessible database of published inscriptions from Israel/Palestine dated ca. 500 BCE to 614 CE. This timespan roughly corresponds to the Persian, Greek, and Roman periods. Our database will make accessible the approximately 10,000 inscriptions published to date and will include substantial contextual information for these inscriptions, including images and geographic information. We tag our data according to Epidoc conventions. </p>
         </projectDesc>
         <classDecl>
            <taxonomy xml:id="IIP-form">
               <category xml:id="tombstone" ana="300005923">
                  <catDesc>Tombstone</catDesc>
               </category>
            </taxonomy>
            <taxonomy xml:id="IIP-materials"/>
            <taxonomy xml:id="IIP-genre">
               <category xml:id="funerary.epitaph">
                  <catDesc>Funerary (Epitaph)</catDesc>
               </category>
            </taxonomy>
            <taxonomy xml:id="IIP-preservation"/>
            <taxonomy xml:id="IIP-writing"/>
            <taxonomy xml:id="IIP-religion">
               <category xml:id="christian">
                  <catDesc>Christian</catDesc>
               </category>
            </taxonomy>
         </classDecl>
      </encodingDesc>
      <profileDesc/>
      <!-- ********* <revisionDesc> ********* -->
      <revisionDesc>
         <change when="2009-04-21" who="persons.xml#Joseph_Faucher">Creation</change>
         <change when="2018-11-28" who="Gaia Lembi">Corrected encoding (abbreviation)</change>
         <change when="2019-01-29" who="persons.xml#Elli_Mylonas">
                adding period attribute to date element, with Periodo value.
            </change>
         <change when="2019-03-12" who="persons.xml#Elli_Mylonas">
                Inserting new titleStmt and PubStmt info
            </change>
      </revisionDesc>
   </teiHeader>
   <facsimile>
      <surface>
         <desc/>
         <graphic url=""/>
      </surface>
   </facsimile>
   <text>
      <body>
         <div type="edition"
              ana="#b1"
              xml:lang="grc"
              subtype="transcription"
              xml:id="elus0018.transcription"
              corresp="#elus0018.translation">
            <p>
               <expan>
                  <abbr>Ζων<unclear>έ</unclear>
                  </abbr>
                  <ex>νος</ex>
               </expan>
            </p>
         </div>
         <div type="translation"
              ana="#b1"
              xml:id="elus0018.translation"
              corresp="#elus0018.transcription">
            <p>Zunain</p>
         </div>
      </body>
      <back>
         <div type="commentary">
            <p>The four letters of this inscription, abbreviating a name, were probably written one each in the four quadrants of a cross.</p>
         </div>
         <div type="bibliography">
            <listBibl>
               <bibl xml:id="b1">
                  <ptr type="biblItem" target="IIP-483.xml"/>
                  <biblScope unit="insc">18</biblScope>
               </bibl>
            </listBibl>
         </div>
      </back>
   </text>
</TEI>
'''


SIMPLE_RELS_EXT_XML = '''<?xml version="1.0" encoding="UTF-8"?>
<rdf:RDF
   xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
   xmlns:fedora-model="info:fedora/fedora-system:def/model#"
>
  <rdf:Description rdf:about="info:fedora/test:1234">
    <fedora-model:hasModel rdf:resource="info:fedora/bdr-cmodel:commonMetadata"/>
  </rdf:Description>
</rdf:RDF>
'''

RELS_EXT_XML = '''<?xml version="1.0" encoding="UTF-8"?>
<rdf:RDF
   xmlns:bul-rel="http://library.brown.edu/#"
   xmlns:fedora-model="info:fedora/fedora-system:def/model#"
   xmlns:fedora-rels-ext="info:fedora/fedora-system:def/relations-external#"
   xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
   xmlns:fabio="http://purl.org/spar/fabio/#"
   xmlns:pso="http://purl.org/spar/pso/#"
>
  <rdf:Description rdf:about="info:fedora/test:1234">
    <bul-rel:displayLabel>user lâbel</bul-rel:displayLabel>
    <bul-rel:hasPagination rdf:datatype="http://www.w3.org/2001/XMLSchema#int">1</bul-rel:hasPagination>
    <bul-rel:panoptoId>12345-abcde</bul-rel:panoptoId>
    <fedora-model:hasModel rdf:resource="info:fedora/bdr-cmodel:commonMetadata"/>
    <fedora-model:hasModel rdf:resource="info:fedora/bdr-cmodel:pdf"/>
    <fedora-rels-ext:isPartOf rdf:resource="info:fedora/test:5555"/>
    <fedora-rels-ext:isMemberOfCollection rdf:resource="info:fedora/test:abcd1234"/>
    <rdf:type rdf:resource="http://purl.org/spar/fabio/DoctoralThesis"/>
    <pso:withStatus rdf:resource="http://purl.org/spar/pso/#embargoed"/>
    <fabio:hasEmbargoDate>2018-06-01T00:00:01Z</fabio:hasEmbargoDate>
    <fabio:hasEmbargoDate>2020-06-01T00:00:01Z</fabio:hasEmbargoDate>
  </rdf:Description>
</rdf:RDF>
'''

OCR_XML = '''<?xml version="1.0" encoding="UTF-8"?>
<mets xsi:schemaLocation="http://www.loc.gov/METS/ http://schema.ccs-gmbh.com/metae/mets-metae.xsd" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="http://www.loc.gov/METS/" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:MODS="http://www.loc.gov/mods/v3" xmlns:mix="http://www.loc.gov/mix/" xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:xlink="http://www.w3.org/TR/xlink" xmlns:mets="http://www.loc.gov/METS/" TYPE="text.ocr.bdh" LABEL="1900-01-04" OBJID="1236281575328533.ocr">
<structMap>
  <div ID="DIVL1" TYPE="Newspaper" ORDER="1">
    <div ID="DIVL2" TYPE="VOLUME">
      <div ID="DIVL3" TYPE="ISSUE" ORDER="1">
        <div ID="DIVL4" TYPE="TITLE_SECTION" ORDER="1">
          <div ID="DIVL5" TYPE="TEXTBLOCK" ORDER="1" LABEL="VOL. IX. No. 72"> </div>
          <div ID="DIVL6" TYPE="TEXTBLOCK" ORDER="1" LABEL="PROVIDENCE, THURSDAY, JANUARY 4, 1900"> </div>
          <div ID="DIVL7" TYPE="TEXTBLOCK" ORDER="1" LABEL="Price Three Cents."> </div>
          <div ID="DIVL8" TYPE="HEADLINE" ORDER="1" LABEL="Brown Daily Herald"> </div>
          <div ID="DIVL9" TYPE="ILLUSTRATION" ORDER="1">
            <div ID="DIVL10" TYPE="IMAGE" ORDER="1" LABEL=""> </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</structMap>
</mets>
'''
