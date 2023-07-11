
DDVUG Challenge 2023 Willibald-Samen (dbt)

Further information regarding the DDVUG Challenge see: dwa-compare.info

This project implements a data warehouse using dbt (data build tool). It provides a framework for defining your data models and transformations, as well as automating the ETL process.


Features

Uses dbt to automate ETL processes and build data models
Supports Snowflake
Implements best practices for data modeling and transformation using dbt
Provides a flexible framework for building custom data models and transformations
Easy to configure and customize to meet your specific needs

Getting Started
- clone the repository and navigate to the project directory.
- install python 3.9
- create venv: python -m venv venv
- upgrade pip: python -m pip install --upgrade pip
- install dbt (with snowflake 1.5.2): pip install -r requirements.txt
- install dependencies: dbt deps
- configure your database connection using the dbt configuration file or edit profiles.yml in source-directory (includes adding three environment-variables).
- run the dbt commands to create your data models and transform your data.
- get the data from https://github.com/m2data/Willibald-Data 
- put the data in your s3-bucket (or azure data lake), configure external tables on snowflake and alter existing definitions (under dwh_01_ext)

                                                           / misc     - kategorie_termintreue - kategorie_termintreue_20220307_20220307_080000.csv ...
- our s3-bucket looks like ddvug-willibald-samen-dbt--ldts-  roadshow - bestellung - bestellung_20220307_20220307_080000.csv ...
                                                           \ webshop  - bestellung - bestellung_20220314_20220314_080000.csv ...
                                                                      \ kunde      - kunde_20220314_20220314_080000.csv
                                                                                     kunde_20220321_20220321_080000.csv                
                                                                                     kunde_20220328_20220328_080000.csv
                                                                      \ ..
Please:
- take a look at the naming-conventions ( useful_documents\naming_convention.md )
- note that our macro are written without dispatcher (they will only work under snowflake)
- note that there are several objects/macros we wrote, that are not supported by datavault4dbt at this time but maybe in the future
- note that the macros we wrote in part depend on the naming-conventions we set up

Install  (newer / other versions not tested)


Contributing
We do not expect contributions to this project. If you have any suggestions please contact us.

License
This work is licensed under a Creative Commons Attribution 4.0 International License: 
http://creativecommons.org/licenses/by/4.0/

Acknowledgements
This project was inspired by the dbt documentation, scalefree and community. We would like to thank the dbt labs team and scalefree.

Disclaimer
THIS SOFTWARE IS PROVIDED BY THE AUTHOR 'AS IS' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.