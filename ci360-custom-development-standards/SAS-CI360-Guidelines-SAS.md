
# Style Guidelines for SAS Code

## Table of Contents
- [General](#general)
- [Naming Conventions](#naming-conventions)
- [Documentation](#documentation)
- [Comments](#comments)
- [Appearance](#appearance)
- [Efficiency](#efficiency)
- [Maintainability](#maintainability)

---

## General

- **Indent code. Use real tabs that equal 4 spaces.**
- **Always use `%do; … %end;` blocks, even if the block is one line.**

```sas
%if 1 = 1 %then %do;
    %put hello world!;
%end;
```

- **Pad parenthesized expressions with spaces.**

```sas
%do %while ( x );
%end;
```

---

## Naming Conventions

It's important to have a consistency in naming. It improves readability. Example:
- **Variable names in DATA steps and PROCs should be uppercase, except temporary variables.**
- **Table and library names should always be lowercase.**
- **Macro variable names should be lowercase**, except globals, automatic variables, or constants.
- **Macro names should be meaningful and lowercase and descriptive.**
- **Avoid sequence numbers in macro names.**
- **Truncate names rather than abbreviating them.**
- **Use underscores for multi-word names; avoid camelCase.**
- **Reserved keywords should be lowercase.**

---

## Documentation

- **Place a macro header at the beginning of every macro using Javadoc-style comments.**

```sas
/**
 * Macro my_macro <br>
 *
 * @author xyz
 * @param first_param description
 * @param second_param description
 */
```

---

## Comments

- **Comments are not a substitute for readable code. Refactor instead of over-commenting.**
- **Explain intent only for tricky or non-standard logic.**
- **Avoid decorative comment blocks in the middle of the code.**
- **Always use C-style comments (`/* */`).**

```sas
/* This instruction needs a comment */
%let very_complicated_instruction = 1;
```

---

## Appearance

- One statement per line.
- Blank line before each DATA and PROC step.
- Left-justify DATA, PROC, RUN and OPTIONS statements.
- Indent logic blocks consistently.
- Refactor if nesting exceeds three levels.
- In a data step, group non-executable statement first (retain, length, keep)

```sas
data foo;
    set boo;
    length a $12 b c 8;
    retain c "";
    keep a c;
    c = b;
run;
```
- Use length statements to avoid data truncations by implicit length assignment.
- In a proc sql, put each sql statement on a different row
- In the select statement of proc sql, the comma goes before the variable name

```sas
proc sql noprint;
    create table foo as
    select a
        , b
        , c
    from moo
    where A = 1;
quit;
```

---

## Efficiency

- Use `KEEP=` and `DROP=` to limit variables.
- Prefer `WHERE` over `IF` when subsetting.
- Use `IF/ELSE` for mutually exclusive conditions.
- Use `IN` instead of multiple OR conditions in a where clause.
- Prefer built-in functions such as `COALESCE`, `SUM`, `MIN`, `MAX`.

```sas
/* use */
AMOUNT = coalesce(MY_AMOUNT, YOUR_AMOUNT);
/* don't use */
if not missing(MY_AMOUNT) then AMOUNT = MY_AMOUNT;
	else AMOUNT = YOUR_AMOUNT;

/* use */
MY_VALUE = max(MY_VALUE, LOWER_BOUNDARY);
/* don't use */
if MY_VALUE < LOWER_BOUNDARY then MY_VALUE = LOWER_BOUNDARY;

/* use */
MY_VALUE = sum(MY_VALUE, INCREASE,BONUS);
/* don't use */
MY_VALUE = MY_VALUE  +  INCREASE + BONUS;
```

- Avoid joins of more than 2 SAS datasets. Data step with the right tables loaded in an hash are more efficient. 
- Avoid Datastep merge. It requires sorted input datasets. Use datastep with hashes or a proc sql.
- Use %sysfunc(fetch()) to loop over the rows of a dataset in macro context.

```sas
%let dsid = %sysfunc(open(dataset_to_loop_on));
%syscall set(dsid);
%do %while(  %sysfunc(fetch(&dsid.)) = 0 );
    /* your macro code here */
%end;
%let dsid = %sysfunc(close(&dsid.));
```

- Reduce read/write cycles by executing as many operations as possible in a single datastep or proc. 
- When reading from an external database table:
    - Filter the table as the first step, to reduce the data transfer
    - Write the filter condition in a WHERE statement, instead of an IF statement
    - Use only SAS functions that can be transferred to the database
    - You can use any SAS function inside a macro function, as only its result is sent to the database. 

```sas
data foo;
	set db_ext.ext_table;
	where REF_DATE = %sysfunc(intnx(month, &REF_DATE., 1));
run;
```

---

## Maintainability

- Define constants using `%LET`.
- Avoid warnings and unnecessary notes in the log.
- Avoid implicit type conversions, use the `PUT` or `INPUT` function.
- Always name the macro in the `%MEND` statement.
- Avoid global macro variables when possible.
- Define local macro variables inside a macro as `%LOCAL`, to prevent nested macros from overwriting them.
- Initialize macro variables before assigning a value via `PROC SQL INTO`.
- In proc sql, use the option `NOPRINT` to eliminate unused data listings.
- Check for errors
    - After every PROC SQL, `&SQLRC. > 4` means an error occurred.
    - After every DATA step or non-sql PROC, `&SYSCC. > 4` means an error occurred.
- Insert meaningful logging messages in the code, to make the log more readable via a reusable macro.
- Split large macros into smaller ones.
- Draw a diagram of how macros get called.
- `%GOTO` statements are unavoidable in SAS, but  use them sparingly.
- Always use the dot to end resolution of a macro variable value.

```sas
%let macro_var2 = &macro_var1.;
```
