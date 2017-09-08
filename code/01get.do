* ---------------------------------------------------------------------
* Project: XX Project Title - XX Step Title
* Program: 01get.do
* Author:  Mauricio Caceres Bravo <caceres@nber.org>
* Created: Mon May 29 22:18:43 EDT 2017
* Updated: Mon May 29 22:18:43 EDT 2017
* Purpose: XX File Purpose
* Note:    XX
* Depends: data/raw
*              XX-raw-file
* Output:  data
*              XX-output-file

* Stata start-up options
* ----------------------

version 13
clear all
set more off
set varabbrev off
capture log close _all
do "00preamble.do"

* Main program wrapper
* --------------------

program main
    syntax, [CAPture NOIsily email]

    * Set up
    * ------

    local  progname XX-project-root/code/XX-code-step/01get
    local  start_time "$S_TIME $S_DATE"
    di "Start: `start_time'"

    * Run the things
    * --------------

    `capture' `noisily' {
        * do stuff
    }
    local rc = _rc

    * If requested, notify via e-mail
    * -------------------------------

    if ("`email'" != "") {
        mail_notify,  rc(`rc')            ///
            email(`c(username)'@nber.org) ///
            progname(`progname')          ///
            start_time(`start_time') `capture'
    }
    exit `rc'
end

* ---------------------------------------------------------------------
* Run the things

main, email cap noi
