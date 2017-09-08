***********************************************************************
*                      Globals for every script                       *
***********************************************************************

global maindir "../"
global datadir "./data"
global outdir  "./out"
global svtable matrix_to_txt, saving($outdir/tables/tables.txt) append format(%21.9f)
global imopts  varn(1) case(preserve) clear

* XX more globals

***********************************************************************
*                            Aux programs                             *
***********************************************************************

* Email program
capture program drop mail_notify
program mail_notify
    syntax, email(str) rc(int) progname(str) start_time(str) [CAPture]
    local end_time "$S_TIME $S_DATE"
    local time     "Start: `start_time'\nEnd: `end_time'"
    if (`rc' == 0) {
        di "End: $S_TIME $S_DATE"
        local paux	  ran [Automated Message]
        local message = "`progname' finished running\n\n`time'"
        local subject = "`progname' `paux'"
    }
    else if ("`capture'" == "") {
        di "WARNING: $S_TIME $S_DATE"
        local paux ran with non-0 exit status [Automated Message]
        local message = "`progname' ran but Stata gave error code r(`rc')\n\n`time'"
        local subject = "`progname' `paux'"
    }
    else {
        di "ERROR: $S_TIME $S_DATE"
        local paux ran with errors [Automated Message]
        local message = "`progname' stopped with error code r(`rc')\n\n`time'"
        local subject = "`progname' `paux'"
    }
    di "`subject'"
    di "`message'"
    shell echo -e "`message'" | mail -s "`subject'" `email'
end

* Wrapper for easy timer use
cap program drop mytimer
program mytimer, rclass
    * args number what step
    syntax anything, [minutes ts]

    tokenize `anything'
    local number `1'
    local what   `2'
    local step   `3'

    if ("`what'" == "end") {
        qui {
            timer clear `number'
            timer off   `number'
        }
        if ("`ts'" == "ts") mytimer_ts `step'
    }
    else if ("`what'" == "info") {
        qui {
            timer off `number'
            timer list `number'
        }
        local seconds = r(t`number')
        local prints  `:di trim("`:di %21.2gc `seconds''")' seconds
        if ("`minutes'" != "") {
            local minutes = `seconds' / 60
            local prints  `:di trim("`:di %21.3gc `minutes''")' minutes
        }
        mytimer_ts Step `step' took `prints'
        qui {
            timer clear `number'
            timer on    `number'
        }
    }
    else {
        qui {
            timer clear `number'
            timer on    `number'
            timer off   `number'
            timer list  `number'
            timer on    `number'
        }
        if ("`ts'" == "ts") mytimer_ts `step'
    }
end

capture program drop mytimer_ts
program mytimer_ts
    display _n(1) "{hline 79}"
    if ("`0'" != "") display `"`0'"'
    display `"        Base: $S_FN"'
    display  "        In memory: `:di trim("`:di %21.0gc _N'")' observations"
    display  "        Timestamp: $S_TIME $S_DATE"
    display  "{hline 79}" _n(1)
end
