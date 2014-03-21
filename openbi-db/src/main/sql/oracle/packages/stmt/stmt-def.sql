CREATE OR REPLACE PACKAGE stmt AUTHID CURRENT_USER AS
  /**
  * Templates for standard ddls
  * APIs to construct list of columns and column definitions
  * $Author: nmarangoni $
  * $Date: 2012-06-08 15:34:39 +0200 (Fr, 08 Jun 2012) $
  * $Revision: 2858 $
  * $Id: $
  * $HeadURL: $
  */

  /**
  * Package spec version string.
  */
  c_spec_version CONSTANT VARCHAR2(1024) := '$Id: $';
  /**
  * Package spec repository URL.
  */
  c_spec_url CONSTANT VARCHAR2(1024) := '$HeadURL: $';
  /**
  * Package body version string.
  */
  c_body_version VARCHAR2(1024);
  /**
  * Package body repository URL.
  */
  c_body_url VARCHAR2(1024);
END stmt;
/
