#!/bin/bash
# This file:
#
#  - Runs functional/integration tests for kafka sink
#
# Exit on error. Append "|| true" if you expect an error.
set -o errexit
# Exit on error inside any functions or subshells.
set -o errtrace
# Do not allow use of undefined vars. Use ${VAR:-} to use an undefined VAR
set -o nounset
# Catch the error in case mysqldump fails (but gzip succeeds) in `mysqldump |gzip`
set -o pipefail
# Turn on traces, useful while debugging but commented out by default
# set -o xtrace

if [[ "${BASH_SOURCE[0]}" != "${0}" ]]; then
  __i_am_main_script="0" # false

  if [[ "${__usage+x}" ]]; then
    if [[ "${BASH_SOURCE[1]}" = "${0}" ]]; then
      __i_am_main_script="1" # true
    fi

    __b3bp_external_usage="true"
    __b3bp_tmp_source_idx=1
  fi
else
  __i_am_main_script="1" # true
  [[ "${__usage+x}" ]] && unset -v __usage
  [[ "${__helptext+x}" ]] && unset -v __helptext
fi

# Set magic variables for current file, directory, os, etc.
__dir="$(cd "$(dirname "${BASH_SOURCE[${__b3bp_tmp_source_idx:-0}]}")" && pwd)"
__root="$(cd "${__dir}"/../../ && pwd)"
__file="${__dir}/$(basename "${BASH_SOURCE[${__b3bp_tmp_source_idx:-0}]}")"
__base="$(basename "${__file}" .sh)"
# shellcheck disable=SC2034,SC2015
__invocation="$(printf %q "${__file}")$( (($#)) && printf ' %q' "$@" || true)"

# Define the environment variables (and their defaults) that this script depends on
LOG_LEVEL="${LOG_LEVEL:-6}" # 7 = debug -> 0 = emergency
NO_COLOR="${NO_COLOR:-}"    # true = disable color. otherwise autodetected

### Functions
##############################################################################

function __b3bp_log () {
  local log_level="${1}"
  shift

  # shellcheck disable=SC2034
  local color_debug="\\x1b[35m"
  # shellcheck disable=SC2034
  local color_info="\\x1b[32m"
  # shellcheck disable=SC2034
  local color_notice="\\x1b[34m"
  # shellcheck disable=SC2034
  local color_warning="\\x1b[33m"
  # shellcheck disable=SC2034
  local color_error="\\x1b[31m"
  # shellcheck disable=SC2034
  local color_critical="\\x1b[1;31m"
  # shellcheck disable=SC2034
  local color_alert="\\x1b[1;37;41m"
  # shellcheck disable=SC2034
  local color_emergency="\\x1b[1;4;5;37;41m"

  local colorvar="color_${log_level}"

  local color="${!colorvar:-${color_error}}"
  local color_reset="\\x1b[0m"

  if [[ "${NO_COLOR:-}" = "true" ]] || { [[ "${TERM:-}" != "xterm"* ]] && [[ "${TERM:-}" != "screen"* ]]; } || [[ ! -t 2 ]]; then
    if [[ "${NO_COLOR:-}" != "false" ]]; then
      # Don't use colors on pipes or non-recognized terminals
      color=""; color_reset=""
    fi
  fi

  # all remaining arguments are to be printed
  local log_line=""

  while IFS=$'\n' read -r log_line; do
    echo -e "$(date -u +"%Y-%m-%d %H:%M:%S UTC") ${color}$(printf "[%9s]" "${log_level}")${color_reset} ${log_line}" 1>&2
  done <<< "${@:-}"
}

function emergency () {                                __b3bp_log emergency "${@}"; exit 1; }
function alert ()     { [[ "${LOG_LEVEL:-0}" -ge 1 ]] && __b3bp_log alert "${@}"; true; }
function critical ()  { [[ "${LOG_LEVEL:-0}" -ge 2 ]] && __b3bp_log critical "${@}"; true; }
function error ()     { [[ "${LOG_LEVEL:-0}" -ge 3 ]] && __b3bp_log error "${@}"; true; }
function warning ()   { [[ "${LOG_LEVEL:-0}" -ge 4 ]] && __b3bp_log warning "${@}"; true; }
function notice ()    { [[ "${LOG_LEVEL:-0}" -ge 5 ]] && __b3bp_log notice "${@}"; true; }
function info ()      { [[ "${LOG_LEVEL:-0}" -ge 6 ]] && __b3bp_log info "${@}"; true; }
function debug ()     { [[ "${LOG_LEVEL:-0}" -ge 7 ]] && __b3bp_log debug "${@}"; true; }

function help () {
  echo "" 1>&2
  echo " ${*}" 1>&2
  echo "" 1>&2
  echo "  ${__usage:-No usage available}" 1>&2
  echo "" 1>&2

  if [[ "${__helptext:-}" ]]; then
    echo " ${__helptext}" 1>&2
    echo "" 1>&2
  fi

  exit 1
}

### Parse commandline options
##############################################################################

# Commandline options. This defines the usage page, and is used to parse cli
# opts & defaults from. The parsing is unforgiving so be precise in your syntax
# - A short option must be preset for every long option; but every short option
#   need not have a long option
# - `--` is respected as the separator between options and arguments
# - We do not bash-expand defaults, so setting '~/app' as a default will not resolve to ${HOME}.
#   you can use bash variables to work around this (so use ${HOME} instead)

# shellcheck disable=SC2015
[[ "${__usage+x}" ]] || read -r -d '' __usage <<-'EOF' || true # exits non-zero when EOF encountered
  -m --msk-version [arg] MSK Config providers library version. Default=0.3.1
  -v               		 Enable verbose mode, print script as it is executed
  -d --debug       		 Enables debug mode
  -h --help        		 This page
  -n --no-color    		 Disable color output
EOF

# shellcheck disable=SC2015
[[ "${__helptext+x}" ]] || read -r -d '' __helptext <<-'EOF' || true # exits non-zero when EOF encountered
 Run tests for Kafka Sink Connector. All tests run in isolation, that is container composition is recreated for each test case
EOF

# Translate usage string -> getopts arguments, and set $arg_<flag> defaults
while read -r __b3bp_tmp_line; do
  if [[ "${__b3bp_tmp_line}" =~ ^- ]]; then
    # fetch single character version of option string
    __b3bp_tmp_opt="${__b3bp_tmp_line%% *}"
    __b3bp_tmp_opt="${__b3bp_tmp_opt:1}"

    # fetch long version if present
    __b3bp_tmp_long_opt=""

    if [[ "${__b3bp_tmp_line}" = *"--"* ]]; then
      __b3bp_tmp_long_opt="${__b3bp_tmp_line#*--}"
      __b3bp_tmp_long_opt="${__b3bp_tmp_long_opt%% *}"
    fi

    # map opt long name to+from opt short name
    printf -v "__b3bp_tmp_opt_long2short_${__b3bp_tmp_long_opt//-/_}" '%s' "${__b3bp_tmp_opt}"
    printf -v "__b3bp_tmp_opt_short2long_${__b3bp_tmp_opt}" '%s' "${__b3bp_tmp_long_opt//-/_}"

    # check if option takes an argument
    if [[ "${__b3bp_tmp_line}" =~ \[.*\] ]]; then
      __b3bp_tmp_opt="${__b3bp_tmp_opt}:" # add : if opt has arg
      __b3bp_tmp_init=""  # it has an arg. init with ""
      printf -v "__b3bp_tmp_has_arg_${__b3bp_tmp_opt:0:1}" '%s' "1"
    elif [[ "${__b3bp_tmp_line}" =~ \{.*\} ]]; then
      __b3bp_tmp_opt="${__b3bp_tmp_opt}:" # add : if opt has arg
      __b3bp_tmp_init=""  # it has an arg. init with ""
      # remember that this option requires an argument
      printf -v "__b3bp_tmp_has_arg_${__b3bp_tmp_opt:0:1}" '%s' "2"
    else
      __b3bp_tmp_init="0" # it's a flag. init with 0
      printf -v "__b3bp_tmp_has_arg_${__b3bp_tmp_opt:0:1}" '%s' "0"
    fi
    __b3bp_tmp_opts="${__b3bp_tmp_opts:-}${__b3bp_tmp_opt}"

    if [[ "${__b3bp_tmp_line}" =~ ^Can\ be\ repeated\. ]] || [[ "${__b3bp_tmp_line}" =~ \.\ *Can\ be\ repeated\. ]]; then
      # remember that this option can be repeated
      printf -v "__b3bp_tmp_is_array_${__b3bp_tmp_opt:0:1}" '%s' "1"
    else
      printf -v "__b3bp_tmp_is_array_${__b3bp_tmp_opt:0:1}" '%s' "0"
    fi
  fi

  [[ "${__b3bp_tmp_opt:-}" ]] || continue

  if [[ "${__b3bp_tmp_line}" =~ ^Default= ]] || [[ "${__b3bp_tmp_line}" =~ \.\ *Default= ]]; then
    # ignore default value if option does not have an argument
    __b3bp_tmp_varname="__b3bp_tmp_has_arg_${__b3bp_tmp_opt:0:1}"
    if [[ "${!__b3bp_tmp_varname}" != "0" ]]; then
      # take default
      __b3bp_tmp_init="${__b3bp_tmp_line##*Default=}"
      # strip double quotes from default argument
      __b3bp_tmp_re='^"(.*)"$'
      if [[ "${__b3bp_tmp_init}" =~ ${__b3bp_tmp_re} ]]; then
        __b3bp_tmp_init="${BASH_REMATCH[1]}"
      else
        # strip single quotes from default argument
        __b3bp_tmp_re="^'(.*)'$"
        if [[ "${__b3bp_tmp_init}" =~ ${__b3bp_tmp_re} ]]; then
          __b3bp_tmp_init="${BASH_REMATCH[1]}"
        fi
      fi
    fi
  fi

  if [[ "${__b3bp_tmp_line}" =~ ^Required\. ]] || [[ "${__b3bp_tmp_line}" =~ \.\ *Required\. ]]; then
    # remember that this option requires an argument
    printf -v "__b3bp_tmp_has_arg_${__b3bp_tmp_opt:0:1}" '%s' "2"
  fi

  # Init var with value unless it is an array / a repeatable
  __b3bp_tmp_varname="__b3bp_tmp_is_array_${__b3bp_tmp_opt:0:1}"
  [[ "${!__b3bp_tmp_varname}" = "0" ]] && printf -v "arg_${__b3bp_tmp_opt:0:1}" '%s' "${__b3bp_tmp_init}"
done <<< "${__usage:-}"

# run getopts only if options were specified in __usage
if [[ "${__b3bp_tmp_opts:-}" ]]; then
  # Allow long options like --this
  __b3bp_tmp_opts="${__b3bp_tmp_opts}-:"

  # Reset in case getopts has been used previously in the shell.
  OPTIND=1

  # start parsing command line
  set +o nounset # unexpected arguments will cause unbound variables
                 # to be dereferenced
  # Overwrite $arg_<flag> defaults with the actual CLI options
  while getopts "${__b3bp_tmp_opts}" __b3bp_tmp_opt; do
    [[ "${__b3bp_tmp_opt}" = "?" ]] && help "Invalid use of script: ${*} "

    if [[ "${__b3bp_tmp_opt}" = "-" ]]; then
      # OPTARG is long-option-name or long-option=value
      if [[ "${OPTARG}" =~ .*=.* ]]; then
        # --key=value format
        __b3bp_tmp_long_opt=${OPTARG/=*/}
        # Set opt to the short option corresponding to the long option
        __b3bp_tmp_varname="__b3bp_tmp_opt_long2short_${__b3bp_tmp_long_opt//-/_}"
        printf -v "__b3bp_tmp_opt" '%s' "${!__b3bp_tmp_varname}"
        OPTARG=${OPTARG#*=}
      else
        # --key value format
        # Map long name to short version of option
        __b3bp_tmp_varname="__b3bp_tmp_opt_long2short_${OPTARG//-/_}"
        printf -v "__b3bp_tmp_opt" '%s' "${!__b3bp_tmp_varname}"
        # Only assign OPTARG if option takes an argument
        __b3bp_tmp_varname="__b3bp_tmp_has_arg_${__b3bp_tmp_opt}"
        __b3bp_tmp_varvalue="${!__b3bp_tmp_varname}"
        [[ "${__b3bp_tmp_varvalue}" != "0" ]] && __b3bp_tmp_varvalue="1"
        printf -v "OPTARG" '%s' "${@:OPTIND:${__b3bp_tmp_varvalue}}"
        # shift over the argument if argument is expected
        ((OPTIND+=__b3bp_tmp_varvalue))
      fi
      # we have set opt/OPTARG to the short value and the argument as OPTARG if it exists
    fi

    __b3bp_tmp_value="${OPTARG}"

    __b3bp_tmp_varname="__b3bp_tmp_is_array_${__b3bp_tmp_opt:0:1}"
    if [[ "${!__b3bp_tmp_varname}" != "0" ]]; then
      # repeatables
      # shellcheck disable=SC2016
      if [[ -z "${OPTARG}" ]]; then
        # repeatable flags, they increcemnt
        __b3bp_tmp_varname="arg_${__b3bp_tmp_opt:0:1}"
        debug "cli arg ${__b3bp_tmp_varname} = (${__b3bp_tmp_default}) -> ${!__b3bp_tmp_varname}"
          # shellcheck disable=SC2004
        __b3bp_tmp_value=$((${!__b3bp_tmp_varname} + 1))
        printf -v "${__b3bp_tmp_varname}" '%s' "${__b3bp_tmp_value}"
      else
        # repeatable args, they get appended to an array
        __b3bp_tmp_varname="arg_${__b3bp_tmp_opt:0:1}[@]"
        debug "cli arg ${__b3bp_tmp_varname} append ${__b3bp_tmp_value}"
        declare -a "${__b3bp_tmp_varname}"='("${!__b3bp_tmp_varname}" "${__b3bp_tmp_value}")'
      fi
    else
      # non-repeatables
      __b3bp_tmp_varname="arg_${__b3bp_tmp_opt:0:1}"
      __b3bp_tmp_default="${!__b3bp_tmp_varname}"

      if [[ -z "${OPTARG}" ]]; then
        __b3bp_tmp_value=$((__b3bp_tmp_default + 1))
      fi

      printf -v "${__b3bp_tmp_varname}" '%s' "${__b3bp_tmp_value}"

      debug "cli arg ${__b3bp_tmp_varname} = (${__b3bp_tmp_default}) -> ${!__b3bp_tmp_varname}"
    fi
  done
  set -o nounset # no more unbound variable references expected

  shift $((OPTIND-1))

  if [[ "${1:-}" = "--" ]] ; then
    shift
  fi
fi


### Automatic validation of required option arguments
##############################################################################

for __b3bp_tmp_varname in ${!__b3bp_tmp_has_arg_*}; do
  # validate only options which required an argument
  [[ "${!__b3bp_tmp_varname}" = "2" ]] || continue

  __b3bp_tmp_opt_short="${__b3bp_tmp_varname##*_}"
  __b3bp_tmp_varname="arg_${__b3bp_tmp_opt_short}"
  [[ "${!__b3bp_tmp_varname}" ]] && continue

  __b3bp_tmp_varname="__b3bp_tmp_opt_short2long_${__b3bp_tmp_opt_short}"
  printf -v "__b3bp_tmp_opt_long" '%s' "${!__b3bp_tmp_varname}"
  [[ "${__b3bp_tmp_opt_long:-}" ]] && __b3bp_tmp_opt_long=" (--${__b3bp_tmp_opt_long//_/-})"

  help "Option -${__b3bp_tmp_opt_short}${__b3bp_tmp_opt_long:-} requires an argument"
done


### Cleanup Environment variables
##############################################################################

for __tmp_varname in ${!__b3bp_tmp_*}; do
  unset -v "${__tmp_varname}"
done

unset -v __tmp_varname


### Externally supplied __usage. Nothing else to do here
##############################################################################

if [[ "${__b3bp_external_usage:-}" = "true" ]]; then
  unset -v __b3bp_external_usage
  return
fi


### Signal trapping and backtracing
##############################################################################

function __b3bp_cleanup_before_exit () {
  cd "${__dir}"
}
trap __b3bp_cleanup_before_exit EXIT

# requires `set -o errtrace`
__b3bp_err_report() {
    local error_code=${?}
    error "Error in ${__file} in function ${1} on line ${2}"
    exit ${error_code}
}
# Uncomment the following line for always providing an error backtrace
# trap '__b3bp_err_report "${FUNCNAME:-.}" ${LINENO}' ERR


### Command-line argument switches (like -d for debugmode, -h for showing helppage)
##############################################################################

# debug mode
if [[ "${arg_d:?}" = "1" ]]; then
  set -o xtrace
  PS4='+(${BASH_SOURCE}:${LINENO}): ${FUNCNAME[0]:+${FUNCNAME[0]}(): }'
  LOG_LEVEL="7"
  # Enable error backtracing
  trap '__b3bp_err_report "${FUNCNAME:-.}" ${LINENO}' ERR
fi

# verbose mode
if [[ "${arg_v:?}" = "1" ]]; then
  set -o verbose
  LOG_LEVEL="7"
fi

# no color mode
if [[ "${arg_n:?}" = "1" ]]; then
  NO_COLOR="true"
fi

# help mode
if [[ "${arg_h:?}" = "1" ]]; then
  # Help exists with code 1
  help "Help using ${0}"
fi


### Validation. Error out if the things required for your script are not present
##############################################################################

[[ "${LOG_LEVEL:-}" ]] || emergency "Cannot continue without LOG_LEVEL. "
export LOG_LEVEL

[[ ! -f "${__root}/target/kafka-sink-graphdb-plugin.zip" ]] && emergency "Cannot find the kafka sink connector plugin. It must be located in ${__root}/target/kafka-sink-graphdb-plugin.zip. Run 'mvn verify' and try again."

### Runtime
##############################################################################
MSK_PLUGIN_DIR="${__root}/target/msk"
info "Building MSK plugin"
rm -rf "${MSK_PLUGIN_DIR}"
mkdir "${MSK_PLUGIN_DIR}"
cp "${__root}/target/kafka-sink-graphdb-plugin.zip" "${MSK_PLUGIN_DIR}"
cd "${MSK_PLUGIN_DIR}"
unzip kafka-sink-graphdb-plugin.zip && rm kafka-sink-graphdb-plugin.zip
cd kafka-sink-graphdb
wget "https://github.com/aws-samples/msk-config-providers/releases/download/r${arg_m}/msk-config-providers-${arg_m}-with-dependencies.zip" -O msk.zip
unzip msk.zip && rm msk.zip
mv lib/*.jar . && rmdir lib
zip -r ../kafka-sink-graphdb-plugin-msk.zip *
rm -rf kafka-sink-graphdb/
cd "${__dir}"
