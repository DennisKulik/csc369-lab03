rm -rf output
rm -rf output_temp
rm -rf output_temp_temp
./gradlew run --args="CountryRequestCount input/access.log input/hostname_country.csv output"

./gradlew run --args="CountryURLCount input/access.log input/hostname_country.csv output"

./gradlew run --args="URLCountryList input/access.log input/hostname_country.csv output" 


Australia,1	/mailman/listinfo/cnc_notice
Australia,1	/twiki/bin/view/Main/SpamAssassinAndPostFix
Canada,2	/
Canada,1	/LateEmail.html
Canada,1	/SpamAssassin.html
Canada,1	/cgi-bin/mailgraph.cgi/mailgraph_3.png
Canada,1	/icons/PythonPowered.png
Canada,1	/icons/gnu-head-tiny.jpg
Canada,1	/icons/mailman.jpg
Canada,1	/images/image004.jpg
Canada,2	/images/image005.jpg
